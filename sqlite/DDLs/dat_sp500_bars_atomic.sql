create table sp500_bars_atomic(
ticker text NOT NULL,
t TEXT NOT NULL,
o REAL NOT NULL,
h REAL NOT NULL,
l REAL NOT NULL,
c REAL NOT NULL,
v REAL NOT NULL,
PRIMARY KEY (ticker, t)
)
;