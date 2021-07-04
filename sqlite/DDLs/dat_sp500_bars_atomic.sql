create table dat_bars_atomic(
ticker text NOT NULL,
t  datetime NOT NULL,
o REAL NOT NULL,
h REAL NOT NULL,
l REAL NOT NULL,
c REAL NOT NULL,
v REAL NOT NULL,
PRIMARY KEY (ticker, t)
)
;