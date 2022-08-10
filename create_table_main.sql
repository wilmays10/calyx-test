CREATE TABLE centro_cultural (
    id serial PRIMARY KEY,
    cod_localidad int,
    id_provincia int,
    id_departamento int,
    categoria varchar (50) check (categoria in ('museo', 'biblioteca', 'cine')),
    provincia varchar (30),
    localidad varchar (50),
    nombre varchar (50),
    domicilio varchar (50),
    codigo_postal varchar (30),
    numero_de_telefono int,
    mail varchar (50),
    web varchar (50)
);