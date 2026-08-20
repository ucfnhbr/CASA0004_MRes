"""Build RMSP POI opportunity grid (H3 res8) from CNEFE 2022.

Downloads the 39 RMSP municipality files from the IBGE FTP, classifies
establishments into opportunity categories via keyword rules on
DSC_ESTABELECIMENTO, and aggregates counts to H3 resolution 8.

Usage: python3 build_poi_grid.py <raw_dir>
Outputs (next to this script):
  poi_points_rmsp.parquet    one row per active establishment (lat, lon, h3, cat)
  poi_grid_res8_rmsp.parquet cell x category counts
"""
import io
import re
import sys
import urllib.request
import zipfile
from pathlib import Path

import h3
import pandas as pd

FTP = ("https://ftp.ibge.gov.br/Cadastro_Nacional_de_Enderecos_para_Fins_Estatisticos/"
       "Censo_Demografico_2022/Arquivos_CNEFE/CSV/Municipio/35_SP/")

RMSP = {
    '3503901': 'ARUJA', '3505708': 'BARUERI', '3506607': 'BIRITIBA-MIRIM',
    '3509007': 'CAIEIRAS', '3509205': 'CAJAMAR', '3510609': 'CARAPICUIBA',
    '3513009': 'COTIA', '3513801': 'DIADEMA', '3515004': 'EMBU',
    '3515103': 'EMBU-GUACU', '3515707': 'FERRAZ_DE_VASCONCELOS',
    '3516309': 'FRANCISCO_MORATO', '3516408': 'FRANCO_DA_ROCHA',
    '3518305': 'GUARAREMA', '3518800': 'GUARULHOS',
    '3522208': 'ITAPECERICA_DA_SERRA', '3522505': 'ITAPEVI',
    '3523107': 'ITAQUAQUECETUBA', '3525003': 'JANDIRA', '3526209': 'JUQUITIBA',
    '3528502': 'MAIRIPORA', '3529401': 'MAUA', '3530607': 'MOGI_DAS_CRUZES',
    '3534401': 'OSASCO', '3539103': 'PIRAPORA_DO_BOM_JESUS', '3539806': 'POA',
    '3543303': 'RIBEIRAO_PIRES', '3544103': 'RIO_GRANDE_DA_SERRA',
    '3545001': 'SALESOPOLIS', '3546801': 'SANTA_ISABEL',
    '3547304': 'SANTANA_DE_PARNAIBA', '3547809': 'SANTO_ANDRE',
    '3548708': 'SAO_BERNARDO_DO_CAMPO', '3548807': 'SAO_CAETANO_DO_SUL',
    '3549953': 'SAO_LOURENCO_DA_SERRA', '3550308': 'SAO_PAULO',
    '3552502': 'SUZANO', '3552809': 'TABOAO_DA_SERRA',
    '3556453': 'VARGEM_GRANDE_PAULISTA',
}

# First match wins. health/education before auto so e.g. AUTO ESCOLA and
# POSTO DE SAUDE are not swallowed by the auto rules.
RULES = [
    ('vacant',    r'\b(VAGO|FECHAD[OA]|DESATIVAD|ABANDONAD|TERRENO|DESOCUPAD|DEMOLI)'),
    ('health',    r'\b(CLINICA|CONSULTORIO|LABORATORIO|ODONTO|DENTISTA|FISIOTERAP|PSICOLOG|VETERINARI|HOSPITAL|POSTO DE SAUDE|TERAPIA)'),
    ('education', r'\b(ESCOLA|CURSO|CRECHE|FACULDADE|UNIVERSIDADE|COLEGIO|IDIOMAS|AUTOESCOLA)'),
    ('food',      r'\b(BAR\b|RESTAURANTE|LANCHONETE|PIZZARIA|PADARIA|ADEGA|CAFE\b|CAFETERIA|CHURRASCARIA|PASTELARIA|SORVETERIA|HAMBURGUER|SUSHI|TEMAKERIA|COZINHA|BEBIDAS|MARMIT|DOCERIA|CONFEITARIA|BOMBONIERE|SALGADO|ACAI|ESPETINHO|EMPORIO|ROTISSERIA|CANTINA)'),
    ('auto',      r'\b(OFICINA|MECANICA|FUNILARIA|LAVA[- ]?(RAPIDO|JATO|CAR)|ESTACIONAMENTO|GARAGEM|AUTO\b|AUTOMOTIV|AUTOPECAS|BORRACHARIA|RETIFICA|POSTO DE (GASOLINA|COMBUSTIVEL)|MOTOS?\b)'),
    ('retail',    r'\b(LOJA|MERCEARIA|MERCAD|COMERCIO|ROUPAS|MOVEIS|PECAS|MATERIA(L|IS)|PRODUTOS|ACOUGUE|HORTIFRUTI|SACOLAO|FARMACIA|DROGARIA|PAPELARIA|BAZAR|BOUTIQUE|CALCADOS|VAREJO|ATACAD|QUITANDA|PET\b|PETSHOP|FLORICULTURA|OTICA|OPTICA|JOALHERIA|LIVRARIA|BRECHO|MODAS|PRESENTES|VARIEDADES|UTILIDADES|ARMARINHO|TABACARIA|BANCA\b|CELULAR|ELETRONICOS|INFORMATICA|COSMETICOS|PERFUMARIA|BIJUTERIA|MAGAZINE|BICICLET)'),
    ('services',  r'\b(SALAO|CABEL|BARBEARIA|BELEZA|ESTETICA|MANICURE|SOBRANCELHA|LAVANDERIA|CHAVEIRO|COSTURA|SAPATARIA|IMOBILIARIA|ADVOCACIA|CONTABILIDADE|ESCRITORIO|CONSULTORIA|CARTORIO|BANCO\b|LOTERIC|CORRETORA|SEGUROS|ASSISTENCIA|GRAFICA|FOTOGRAF|FUNERARIA|AGENCIA|DESPACHANTE|DEDETIZA|LIMPEZA|MANUTENCAO|REFRIGERACAO|ELETRICISTA|ENCANADOR|SERVICOS|ATELIE|TATUAGEM|TATTOO|STUDIO|ESTUDIO|CORREIOS|TELEFONIA|PINTURA)'),
    ('industry',  r'\b(FABRICA|DEPOSITO|GALPAO|MARCENARIA|SERRALHERIA|METALURGIC|INDUSTRIA|CONFECCAO|TRANSPORTADORA|LOGISTICA|DISTRIBUIDORA|VIDRACARIA|MADEIREIRA|TORNEARIA|USINAGEM|RECICLA|FERRO[- ]?VELHO|SUCATA|MARMORARIA|GESSO|CALHAS|TAPECARIA|ESTOFA|EMBALAGEN)'),
    ('leisure',   r'\b(ACADEMIA|CLUBE|TEATRO|CINEMA|MUSEU|QUADRA|GINASIO|PARQUE|BUFFET|EVENTOS|DANCETERIA|HOTEL|POUSADA|HOSTEL|MOTEL|PILATES|CROSSFIT|LAN[- ]?HOUSE|PESQUEIRO)'),
]
COMPILED = [(cat, re.compile(pat)) for cat, pat in RULES]

USECOLS = ['COD_MUNICIPIO', 'LATITUDE', 'LONGITUDE', 'NV_GEO_COORD',
           'COD_ESPECIE', 'DSC_ESTABELECIMENTO']
ESPECIE_FIXED = {4: 'education', 5: 'health', 8: 'religious'}


def classify(text, especie):
    if especie in ESPECIE_FIXED:
        return ESPECIE_FIXED[especie]
    for cat, rx in COMPILED:
        if rx.search(text):
            return cat
    return 'other'


def load_municipality(code, name, raw_dir):
    csv_path = raw_dir / f'{code}_{name}.csv'
    if not csv_path.exists():
        url = f'{FTP}{code}_{name}.zip'
        print(f'downloading {name}...', flush=True)
        data = urllib.request.urlopen(url, timeout=300).read()
        with zipfile.ZipFile(io.BytesIO(data)) as z:
            member = z.namelist()[0]
            csv_path.write_bytes(z.read(member))
    df = pd.read_csv(csv_path, sep=';', usecols=USECOLS,
                     dtype={'COD_ESPECIE': 'int8', 'NV_GEO_COORD': 'int8',
                            'DSC_ESTABELECIMENTO': 'string'})
    est = df[df.COD_ESPECIE.isin([4, 5, 6, 8])].copy()
    txt = est.DSC_ESTABELECIMENTO.str.upper().fillna('')
    est['cat'] = [classify(t, e) for t, e in zip(txt, est.COD_ESPECIE)]
    est = est[est.cat != 'vacant'].dropna(subset=['LATITUDE', 'LONGITUDE'])
    est['h3'] = [h3.latlng_to_cell(la, lo, 8)
                 for la, lo in zip(est.LATITUDE, est.LONGITUDE)]
    print(f'{name}: {len(est):,} active establishments', flush=True)
    return est[['COD_MUNICIPIO', 'LATITUDE', 'LONGITUDE', 'NV_GEO_COORD', 'cat', 'h3']]


def main():
    raw_dir = Path(sys.argv[1])
    raw_dir.mkdir(parents=True, exist_ok=True)
    out_dir = Path(__file__).parent

    parts = [load_municipality(c, n, raw_dir) for c, n in RMSP.items()]
    points = pd.concat(parts, ignore_index=True)
    points.to_parquet(out_dir / 'poi_points_rmsp.parquet')

    grid = pd.crosstab(points.h3, points.cat)
    grid.to_parquet(out_dir / 'poi_grid_res8_rmsp.parquet')

    print(f'\ntotal: {len(points):,} establishments, {len(grid):,} res8 cells')
    print(points.cat.value_counts().to_string())
    print(f"other share: {(points.cat == 'other').mean() * 100:.1f}%")


if __name__ == '__main__':
    main()
