#!/usr/bin/python
# -*- coding: utf-8 -*-

# Standard library
import datetime
import logging
import sys
import os

# External library
import pandas as pd
from settings import USERNAME, PASSWORD, PORT, HOST, DB
from slugify import slugify
from sqlalchemy import create_engine, MetaData
from sqlalchemy.exc import OperationalError, ProgrammingError, DataError
from sqlalchemy.orm import sessionmaker
import requests


logger = logging.getLogger("").getChild(__name__)

class Client:

    def __init__(self):

        self.engine = None
        self.session = None
        self.meta_data = None
        self.connected = False

    def connect(self, database='calyx', user='calyx_dba', password='calyx',
                     host='localhost'):
        """
        Establece la conexión a la base de datos
        :param database: nombre de la base de datos
        :param user: usuario de la base de datos
        :param password: clave de la base de datos
        :param host: host de la base de datos
        :return: None, guarda la conexión en self.con
        """
        result = False
        try:
            url_db = f'{USERNAME}:{PASSWORD}@{HOST}:{PORT}/{DB}'
            engine = create_engine(f'postgresql://{url_db}')
            Session = sessionmaker(bind=engine)
            self.engine = engine
            self.session = Session()

            self.connected = True
            logger.info('Conexión a la base de datos establecida')

        except OperationalError as e:
            logger.error(f'Error al conectarse a la base de datos: {e}')
            sys.exit(1)

        return self.connected

    def create_metadata(self):
        """
        Crea el objeto metadata para acceder a las tablas de la bd
        :return: None
        """
        self.meta_data = MetaData(bind=self.engine)
        MetaData.reflect(self.meta_data)
        return

    def execute(self, raw):
        if self.connected:
            try:
                self.engine.execute(raw)
                self.session.commit()
                logger.info('Consulta ejecutada con éxito.')
            except OperationalError as e:
                logger.error(f'Error al conectarse a la base de datos: {e}')
                sys.exit(1)
            except ProgrammingError as e:
                logger.info(f'La tabla que intenta crear ya existe: {e}')

        return

    def create_table(self, file):
        """
        Crea una tabla a partir de un archivo .sql
        :param file: nombre del archivo con las instrucciones sql
        :return: None
        """
        if self.connected:
            try:
                with open(file, 'r') as create_table:
                    data = create_table.read()
                    self.execute(data)
            except IOError as e:
                logger.error(f'Error al acceder al archivo {file}')

        return

    def load_data(self, file):
        """
        Carga la base de datos con los datos desde un archivo csv
        :param file: nombre del archivo csv
        :return: None
        """
        if self.connected:
            logger.info(f'Procesando archivo {file}')

            # ex: museos/2022-August/museos-09-08-2022.csv
            file_name = file.split('/')[2].split('-')
            cols = ['cod_loc', 'idprovincia', 'iddepartamento',
                    'categoria', 'provincia', 'localidad', 'nombre',
                    'direccion', 'cp', 'telefono', 'mail', 'web', 'fuente'
                    ]
            # crea el dataframe a partir del archivo descargado
            df = pd.read_csv(file, delimiter=',')

            # los nombres de las columnas se pasan a minusculas
            df.set_axis([slugify(name, separator='_') for name in df], axis=1, inplace=True)

            # Elimina la columna categoria y subcategoria ya que el valor será
            # de acuerdo al nombre del archivo.
            try:
                df.drop(['categoria'], axis=1, inplace=True)
                df.drop(['subcategoria'], axis=1, inplace=True)
            except KeyError as e:
                logger.info(f'No existe la columna: {e}')

            df.rename({'domicilio': 'direccion'}, axis=1, inplace=True)

            new_df = df.assign(categoria=file_name[0])

            # carga de datos a la tabla
            try:
                new_df[cols].to_sql('centro_cultural', con=self.engine,
                                    if_exists='append', index=False)
            except DataError as e:
                logger.error(f'Error al cargar dato de {file}: {e}')
                pass
            except KeyError as e:
                logger.error(f'Error cargando dato de {file}: {e}')

        return

    def get_data(self, category, url):
        """
        Obtiene el archivo csv de la url dada y lo guarda localmente
        :param url: url desde donde se descarga el archivo
        :param category: nombre de la categoría a la que pertenece el csv
        :return: None
        """
        today = datetime.datetime.today()
        # crea el directorio en caso de que no exista
        os.makedirs(f'{category}/{today.year}-{today.strftime("%B")}', exist_ok=True)
        # nombre del archivo
        name = f'{category}/{today.year}-{today.strftime("%B")}/{category}-' \
               f'{today.strftime("%d-%m-%Y")}.csv'

        # descarga y guarda localmente
        logger.info(f'Descargando archivo csv en {name}')
        with open(name, 'wb') as f,\
                requests.get(url, stream=True) as r:

            for line in r.iter_lines():
                f.write(line+'\n'.encode())

            logger.info('Archivo guardado con éxito.')

        return
