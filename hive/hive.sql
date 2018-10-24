/* Creating tables*/
CREATE TABLE escuela(region STRING, distrito STRING, ciudad STRING, idescuela INT, nombreescuela STRING, nivel STRING, serie INT)
    > row format delimited
    > fields terminated by ',';

CREATE TABLE estudiante(region STRING, distrito STRING, idescuela INT, nombreescuela STRING, nivel STRING, sexo STRING, idestudiante INT)
    > row format delimited
    > fields terminated by ',';


/* Q1*/
SELECT estudiante.region, escuela.ciudad, count(*) 
FROM estudiante, escuela 
WHERE escuela.idescuela = estudiante.idescuela 
GROUP BY estudiante.region, escuela.ciudad; 

/* Q2*/
SELECT escuela.ciudad, escuela.nivel, count(*) 
FROM escuela 
GROUP BY escuela.ciudad, escuela.nivel;

/* Q3*/
SELECT count(*) 
FROM escuela, estudiante 
WHERE escuela.idescuela = estudiante.idescuela AND escuela.ciudad = 'Ponce' AND escuela.nivel ='Superior' AND estudiante.sexo = 'F';

/* Q4*/
SELECT escuela.region, escuela.distrito, escuela.ciudad, count(*)  
FROM escuela, estudiante 
WHERE escuela.idescuela = estudiante.idescuela AND estudiante.sexo = 'M' 
GROUP BY escuela.region, escuela.distrito, escuela.ciudad;