#!/usr/bin/python
# -*- coding: utf-8 -*-

# Standard library
from settings import BIBLIOTECAS, CINES, MUSEOS
import logging
import os

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
    bd_ok = cliente.connect()

    if bd_ok:
        logging.debug('Descargando información')
        logging.info('Descargando información')
        #for k,v in urls.items():
        #    cliente.get_data(k, v)

        # crea tabla principal en la bd
        cliente.create_table('create_table_main.sql')

        logging.debug('Procesando información')
        logging.info('Procesando información')

        for k in urls.keys():
            for name_dir, dirs, files in os.walk(f'{k}'):
                for name_file in files:
                    logging.debug(f'Cargando datos de {name_file}')
                    logging.info(f'Cargando datos de {name_file}')
                    cliente.load_data(f'{name_dir}/{name_file}')

    logging.debug('Fin main script')
    logging.info('Fin main script')

    logging.shutdown()
    return


if __name__ == '__main__':
    main()
