# Saffron Lane data

Everything in `raw/` is **synthetic**. The author generated the original files with Faker
(seed 42) and a custom script. Saffron Lane is a fictional shop. Any resemblance to real
people, companies or products is coincidental.

- `raw/day1/` is one night's export of five tables: `transactions.csv` (750 orders),
  `customers.csv` (200), `products.csv` (60), `pg_data.csv` (750 payments) and
  `cashback_rewards.csv` (391).
- `raw/day2/` is the next night's export. Four of the tables gain an `updated_at` column, and
  some rows changed: order statuses, reward statuses, product prices and customer devices.
  `pg_data.csv` is the same as on day 1.

## This copy is sanitised

The files were copied from the author's synthetic dataset and then cleaned so that nothing in
them is a real email address, phone number, IP address, payment handle or brand. The rest of each file is unchanged: row order,
column order, quoting, line endings, and every count, status, date and amount.

| Column | Rule |
|---|---|
| `customers.email` | the part before the `@` is kept; the domain is always `example.com` |
| `customers.phone_number` | removed (so `customers.csv` has 6 columns, 7 on day 2) |
| `pg_data.pg_metadata` `ip_address` | replaced by an address from the documentation range `192.0.2.1` to `192.0.2.254`, in order of first appearance (the 750 addresses wrap around, so some repeat) |
| `pg_data.pg_metadata` `merchant_vpa` | `saffronlane@upi` (empty stays empty) |
| `pg_data.gateway_name` | the four gateway names are invented names |
| `products.brand`, `products.product_name` | the 39 brands are invented names and product names are generic descriptions, still one per product; day 1 and day 2 use the same names |

`pg_metadata.bank_reference_no` is left as the generator made it: a four-letter code followed by random digits, invented for this data. The book never prints it.

The book never prints an email, phone number, IP address, merchant VPA or bank reference.
Chapter 4 masks the email column before anything is shown.
