# Data

This folder is intentionally empty in the repository.

When you run the notebook, the following files are created automatically:

| File | Description |
|------|-------------|
| `openfoodfacts_data.json` | Raw API response (~50,000 products) |
| `valid_products_full.csv` | Filtered products with complete nutrition data |
| `clean_products.csv` | Final cleaned dataset with engineered features |
| `food_data.db` | Normalised SQLite database (5 tables) |

All data comes from the [Open Food Facts API](https://world.openfoodfacts.org/)
and is licensed under [ODbL](https://opendatacommons.org/licenses/odbl/1-0/).
