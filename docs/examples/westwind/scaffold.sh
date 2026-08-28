#!/usr/bin/env bash
# Write the eight Westwind manifests into migrations/ with `smugglr migrate new`.
# Run once, commit the output; make.sh applies whatever is in migrations/.
# The order of the specs is the order make.sh applies them.
set -euo pipefail
here="$(cd "$(dirname "$0")" && pwd)"
smugglr="${SMUGGLR:-smugglr}"
mkdir -p "$here/migrations"
rm -f "$here/migrations"/*.json

n=0
scaffold() {
  n=$((n + 1))
  local name="$1"; shift
  "$smugglr" migrate new "$name" "$@" > "$here/migrations/$(printf '%02d' "$n")_$name.json"
  echo "migrations/$(printf '%02d' "$n")_$name.json"
}

scaffold create_categories    id:pk:notnull name:text:notnull:unique description:text updated_at:int:notnull
scaffold create_suppliers     id:pk:notnull company:text:notnull contact:text city:text region:text updated_at:int:notnull
scaffold create_products      id:pk:notnull name:text:notnull supplier_id:text:notnull category_id:text:notnull:index unit:text unit_price:real:notnull in_stock:int:notnull:default=0 discontinued:int:notnull:default=0 updated_at:int:notnull
scaffold create_customers     id:pk:notnull code:text:notnull:unique company:text:notnull contact:text title:text city:text region:text updated_at:int:notnull
scaffold create_employees     id:pk:notnull code:text:notnull:unique last_name:text:notnull first_name:text:notnull title:text reports_to:text city:text updated_at:int:notnull
scaffold create_shippers      id:pk:notnull company:text:notnull phone:text updated_at:int:notnull
scaffold create_orders        id:pk:notnull customer_id:text:notnull:index employee_id:text:notnull shipper_id:text:notnull order_date:text:notnull:index required_date:text shipped_date:text freight:real:notnull:default=0 ship_city:text ship_region:text updated_at:int:notnull
scaffold create_order_details id:pk:notnull order_id:text:notnull:index product_id:text:notnull unit_price:real:notnull quantity:int:notnull discount:real:notnull:default=0 updated_at:int:notnull
