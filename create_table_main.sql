CREATE TABLE centro_cultural (
    id serial PRIMARY KEY,
    cod_loc int,
    idprovincia int,
    iddepartamento int,
    categoria varchar (50) check (categoria in ('museos', 'bibliotecas', 'cines')),
    provincia varchar (70),
    localidad varchar (100),
    nombre varchar (120),
    direccion varchar (150),
    cp varchar (30),
    telefono varchar(30),
    mail varchar (80),
    web varchar (200)
);