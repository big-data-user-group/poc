
CREATE DATABASE `poc` DEFAULT CHARACTER SET utf8;

use poc;

CREATE TABLE `sale` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `office_location_id` bigint(20) NOT NULL,
  `product_id` bigint(20) NOT NULL,
  `sold_price` decimal(10,2) NOT NULL,
  `date` datetime NOT NULL,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8

