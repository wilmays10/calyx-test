CREATE TABLE centro_cultural (
    id serial PRIMARY KEY,
    cod_loc INT,
    idprovincia INT,
    iddepartamento INT,
    categoria VARCHAR (50) CHECK (categoria IN ('museos', 'bibliotecas', 'cines')),
    provincia VARCHAR (70),
    localidad VARCHAR (100),
    nombre VARCHAR (120),
    direccion VARCHAR (150),
    cp VARCHAR (30),
    telefono VARCHAR(30),
    mail VARCHAR (80),
    web VARCHAR (200),
    fuente VARCHAR (130)
);