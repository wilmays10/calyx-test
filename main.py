#!/usr/bin/python
# -*- coding: utf-8 -*-

# Standard library
from decouple import config
import logging
import pandas as pd
import os

# External library
from slugify import slugify
from sqlalchemy import func, insert

from client import Client


# logger main
logging.basicConfig(
    format='%(asctime)-5s %(name)-10s %(levelname)-8s %(message)s',
    level=logging.INFO,
    filemode="a"
)
# para no duplicar logs
if logging.getLogger('').hasHandlers():
    logging.getLogger('').handlers.clear()


def info_cines(cliente, file):
    """
    Calcula los datos para popular la tabla cine
    :param cliente: conex a la base de datos. Type Client
    :param file: nombre del archivo csv a procesar
    :return:
    """
    if cliente.connected:
        logging.info(f'Procesando archivo {file}')
        # ex: cines/2022-August/museos-09-08-2022.csv
        cols = ['provincia', 'pantallas', 'butacas', 'espacio_incaa']

        # crea el dataframe a partir del archivo descargado
        df = pd.read_csv(file, delimiter=',')

        # los nombres de las columnas se pasan a minusculas
        df.set_axis([slugify(name, separator='_') for name in df], axis=1, inplace=True)
        df = df[cols]

        # normalizamos para poder sumar
        df['espacio_incaa'] = df['espacio_incaa'].fillna(0)
        df['espacio_incaa'].replace('SI', 'si', inplace=True)
        df['espacio_incaa'].replace('Si', 'si', inplace=True)
        df['espacio_incaa'].replace('si', 1, inplace=True)
        df['espacio_incaa'] = pd.to_numeric(df['espacio_incaa'])

        # carga de datos a la tabla
        try:
            df.groupby(['provincia']).sum().to_sql('cine', con=cliente.engine,
                                                   if_exists='append')
        except KeyError as e:
            logging.error(f'Error cargando dato de {file}: {e}')
    return


def resume(cliente):
    """
    Realiza las consultas correspondientes para popular la tabla resume
    :param cliente: Objeto Client con conexión a la base de datos
    :return: None
    """

    centro_cultural_table = cliente.meta_data.tables['centro_cultural']
    resume_table = cliente.meta_data.tables['resume']
    categoria_col = centro_cultural_table.columns.get('categoria')
    fuente_col = centro_cultural_table.columns.get('fuente')
    prov_col = centro_cultural_table.columns.get('provincia')
    result_cat = cliente.session.query(
        categoria_col,
        func.count(categoria_col)
    ).group_by(categoria_col).all()
    result_fuente = cliente.session.query(
        fuente_col,
        func.count(fuente_col)
    ).group_by(fuente_col).all()
    result_prov = cliente.session.query(
        prov_col,
        categoria_col,
        func.count()
    ).group_by(prov_col, categoria_col).all()

    logging.debug('Cargando datos de resumen.')
    logging.info('Cargando datos de resumen.')
    for r in result_cat:
        stmt = insert(resume_table).values(categoria=r[0], total=r[1])
        cliente.execute(stmt)
    for r in result_fuente:
        stmt = insert(resume_table).values(fuente=r[0], total=r[1])
        cliente.execute(stmt)
    for r in result_prov:
        stmt = insert(resume_table).values(provincia=r[0],
                                           categoria=r[1],
                                           total=r[2])
        cliente.execute(stmt)

    return


def main():
    urls = {
        'museos': config('MUSEOS'),
        'cines': config('CINES'),
        'bibliotecas': config('BIBLIOTECAS')
    }

    # Se añaden dos nuevos handlers al root logger, uno para los niveles de debug o
    # superiores y otro para que se muestre por pantalla los niveles de info o
    # superiores.
    file_debug_handler = logging.FileHandler('logs_debug.log')
    file_debug_handler.setLevel(logging.DEBUG)
    file_debug_format = logging.Formatter('%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s')
    file_debug_handler.setFormatter(file_debug_format)
    logging.getLogger('').addHandler(file_debug_handler)

    consola_handler = logging.StreamHandler()
    consola_handler.setLevel(logging.INFO)
    consola_handler_format = logging.Formatter('%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s')
    consola_handler.setFormatter(consola_handler_format)
    logging.getLogger('').addHandler(consola_handler)

    logging.debug('Inicio main script')
    logging.info('Inicio main script')

    cliente = Client()

    # conexión a la base de datos, se puede pasar por args nombre de la base, user, passw y host
    logging.debug('Inicio main script')
    logging.info('Inicio main script')

    logging.debug('Descargando información')
    logging.info('Descargando información')
    # for k,v in urls.items():
    #    cliente.get_data(k, v)

    bd_ok = cliente.connect()

    if bd_ok:
        # crea tabla principal en la bd
        logging.debug('Creando tablas...')
        logging.info('Creando tablas...')
        cliente.create_table('create_table_main.sql')
        cliente.create_table('create_table_resume.sql')
        cliente.create_table('create_table_cines.sql')

        logging.debug('Procesando información')
        logging.info('Procesando información')

        for k in urls.keys():
            for name_dir, dirs, files in os.walk(f'{k}'):
                for name_file in files:
                    logging.debug(f'Cargando datos de {name_file}')
                    logging.info(f'Cargando datos de {name_file}')
                    cliente.load_data(f'{name_dir}/{name_file}')

        cliente.create_metadata()
        logging.debug('Realizando consultas')
        logging.info('Realizando consultas')
        resume(cliente)

        logging.debug('Procesando información de cines')
        logging.info('Procesando información de cines')
        for name_dir, dirs, files in os.walk('cines'):
            for name_file in files:
                info_cines(cliente, f'{name_dir}/{name_file}')

    logging.debug('Fin main script')
    logging.info('Fin main script')

    logging.shutdown()
    return


if __name__ == '__main__':
    main()
