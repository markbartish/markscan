/* 
PRAGMA foreign_keys=OFF;
BEGIN TRANSACTION;
CREATE TABLE hashkeys  (id INTEGER PRIMARY KEY, hashkey INTEGER NOT NULL UNIQUE);
INSERT INTO hashkeys VALUES(1,123342);
INSERT INTO hashkeys VALUES(2,12334);
CREATE TABLE paths     (id INTEGER PRIMARY KEY, hashkey_id INTEGER NOT NULL, fsize INTEGER NOT NULL, path VARCHAR(4096) UNIQUE, FOREIGN KEY(hashkey_id) REFERENCES hashkeys(id));
INSERT INTO paths VALUES(1,1,12,'/dev/null');
INSERT INTO paths VALUES(2,1,24,'/dev/nullity');
INSERT INTO paths VALUES(3,2,32,'/dev/nullitet');
CREATE TABLE toprune   (id INTEGER PRIMARY KEY, path_id INTEGER NOT NULL UNIQUE, FOREIGN KEY(path_id) REFERENCES paths(id));
COMMIT;

 */

SELECT hashkeys.hashkey AS hkey, path FROM paths INNER JOIN hashkeys ON hashkeys.id = paths.hashkey_id WHERE hashkey_id IN (
    SELECT hashkey_id FROM (SELECT paths.hashkey_id, COUNT(path) AS cpaths FROM paths GROUP BY paths.hashkey_id) WHERE cpaths > 1)
;