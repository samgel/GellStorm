create table def_tickers(
exchange text NOT NULL,
ticker text NOT NULL,
name NOT NULL,
PRIMARY KEY (ticker, exchange)
)
;