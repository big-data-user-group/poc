CREATE database poc;

CREATE TABLE poc.sale (
  id bigint NOT NULL,
  office_location_id bigint NOT NULL,
  product_id bigint NOT NULL,
  sold_price decimal NOT NULL,
  date timestamp NOT NULL,
  PRIMARY KEY (id)
);
