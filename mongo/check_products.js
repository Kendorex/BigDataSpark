const assert = require("assert").strict;
const analytics = db.getSiblingDB("analytics");
const products = analytics.getCollection("report_products");

print("Top 10 product names by units (source contains only 3 names):");
printjson(products.find(
    {row_type: "product", sales_rank: {$lte: 10}},
    {_id: 0, product_name: 1, units: 1, revenue: 1, sales_rank: 1}
).sort({sales_rank: 1}).limit(10).toArray());

print("Revenue by category:");
printjson(products.find(
    {row_type: "category"},
    {_id: 0, product_category: 1, revenue: 1}
).sort({revenue: -1, product_category: 1}).toArray());

print("Average ratings and reported review counts:");
printjson(products.find(
    {row_type: "product"},
    {_id: 0, product_name: 1, avg_rating: 1, review_count: 1}
).sort({product_name: 1}).toArray());

assert.equal(products.countDocuments({}), 6);
assert.equal(products.countDocuments({row_type: "product"}), 3);
assert.equal(products.countDocuments({row_type: "category"}), 3);
assert.equal(products.countDocuments({revenue: {$type: "decimal"}}), 6);

// Product and category subtotals overlap: validate each row_type separately.
for (const rowType of ["product", "category"]) {
    const totals = products.aggregate([
        {$match: {row_type: rowType}},
        {$group: {_id: null, sales: {$sum: "$sales_count"}, units: {$sum: "$units"}, revenue: {$sum: "$revenue"}}}
    ]).toArray();
    assert.equal(totals.length, 1);
    assert.equal(totals[0].sales.toString(), "10000");
    assert.equal(totals[0].units.toString(), "54623");
    assert.equal(totals[0].revenue.toString(), "2529852.12");
}
print("MONGOSH CHECK OK: 6 documents, both subtotals match 10000 sales / 54623 units / 2529852.12");
