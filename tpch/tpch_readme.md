%md

# 📦 Real-World Business Problem (Based on TPCH Data)

## 🏢 Scenario
You work for a global supply chain company.  
You are asked to build a reporting dataset that will be used by business analysts to:  
- Track sales performance of parts.  
- Understand supplier efficiency.  
- See customer purchase trends over time.  
- Calculate profitability after discounts and taxes.  
- Analyze regional sales trends.  

You are building this dataset for the analytics team who will then build dashboards and reports on it.

## 🎯 Requirements

### 1. Business Metrics to Calculate

| Metric          | Description                                         |
|------------------|-----------------------------------------------------|
| **Gross Sales** | `SUM(lineitem.extendedPrice)`                        |
| **Discount Amount** | `SUM(lineitem.extendedPrice * lineitem.discount)` |
| **Tax Amount**  | `SUM(net sales * lineitem.tax)`                      |
| **Net Sales**   | `SUM(lineitem.extendedPrice - (discount + tax))`       |
| **Profit** | `Net Sales - Unit Price`                           |
| **Profit Margin** | `(Final Revenue - total cost) / Final Revenue`     |
*(Total cost can be assumed from supplier if you want extra difficulty.)*

### 2. Columns to Output
- `orderDate`
- `customerName`
- `customerSegement`
- `supplierName`
- `nationName`
- `regionName`
- `partType`
- `partName`
- `orderPriority`
- `grossSales`
- `discountAmount`
- `taxAmount`
- `netSales`
- `profit`
- `profitMargin`
- `customerRunningRevenue` (running sum per customer by order date)

### 3. Data Modeling Rules
Start from **orders** (primary grain = order line level).  
Join to:
- **customer** (`customerKey`)
- **lineitem** (`orderKey`)
- **part** (`partKey`)
- **supplier** (`supplierKey`)
- **nation** → **region** (`nationKey`, `regionKey`)  

---

## 🛠 Technical Requirements

| Requirement        | How                                                   |
|---------------------|-------------------------------------------------------|
| **Joins**          | Multiple joins, proper keys                           |
| **Column transformations** | Compute net sales, tax, etc                    |
| **Window functions** | Running total per customer                          |
| **Aggregations**   | Sum at the order-customer level                        |
| **Filters**        | Year, part type, region                               |

---

## 🧠 Design Flow Suggestion

1. Start from **orders**.  
2. Join **customer**.  
3. Join **lineitem**.  
4. Join **part** (for part type filter).  
5. Join **supplier**.  
6. Join **nation + region**.  
7. Apply filters.  
8. Add computed columns.  
9. Add window functions.  
10. Final select.