CREATE TABLE PLAYERS(ID string, FIRSTNAME string, LASTNAME string, FIRSTSEASON int, LASTSEASON int, WEIGHT int, BIRTHDATE date);

SELECT *
FROM PLAYERS
WHERE WEIGHT<135;
