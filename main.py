#!/usr/bin/python
# -*- coding: utf-8 -*-

# Standard library
from settings import BIBLIOTECAS, CINES, MUSEOS
import logging
import os

# External library
from sqlalchemy import func, insert

from client import Client


def main():
    urls = {
        'museos': MUSEOS,
        'cines': CINES,
        'bibliotecas': BIBLIOTECAS
    }
    # logger main
    logging.basicConfig(
        format='%(asctime)-5s %(name)-15s %(levelname)-8s %(message)s',
        level=logging.INFO,
        filemode="a"
    )
    # para no duplicar logs
    if logging.getLogger('').hasHandlers():
        logging.getLogger('').handlers.clear()

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

        logging.debug('Procesando información')
        logging.info('Procesando información')

        for k in urls.keys():
            for name_dir, dirs, files in os.walk(f'{k}'):
                for name_file in files:
                    logging.debug(f'Cargando datos de {name_file}')
                    logging.info(f'Cargando datos de {name_file}')
                    cliente.load_data(f'{name_dir}/{name_file}')

        cliente.create_metadata()

        logging.debug('Realizando consultas.')
        logging.info('Realizando consultas.')
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


    logging.debug('Fin main script')
    logging.info('Fin main script')

    logging.shutdown()
    return


if __name__ == '__main__':
    main()
