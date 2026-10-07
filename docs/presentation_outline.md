# Presentation outline (5–7 min, ~9 slides)

1. **Title** — team, customer profile: *Marketing*, repo link (QR code).
2. **Team & responsibilities** — table from README (who did what).
3. **Architecture** — source → bronze → silver (+quarantine) → gold → ops; parameters `catalog/env/source`
   for portability; Job from `databricks.yml`. (~40 s)
4. **Silver ER diagram** — screenshot from Catalog Explorer; 1 sentence why it is 3NF; how DQ is enforced
   (rules → quarantine, CHECK/NOT NULL, PK/FK + validation). (~50 s)
5. **Gold** — `customer_activity` (1 row/customer, LEFT JOIN!) and the marts built on it. (~30 s)
6. **Answers Q1–Q3** — dashboard screenshot: BUILDING activation rate, new customers in 1996-Q1
   (+ quarterly chart explaining why it's ~0), repeat-rate ranking. (~60 s)
7. **Answer Q4** — cohort chart; why 1996 cohorts are empty; alternative "activity cohort" view. (~40 s)
8. **Demo: validation & monitoring** — run `04_validation` (all green), show `validation_results`;
   in `06_monitoring` show alert A1 firing on artificially reduced BUILDING data. (~90 s)
9. **Challenges & lessons** — zero-order customers vs inner join, censoring, informational FKs,
   no signup date. Thank you / questions.
