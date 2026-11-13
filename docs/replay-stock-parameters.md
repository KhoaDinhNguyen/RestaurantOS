# Replay and stock parameters

Why starting stock comes from a warm-up window, and what W, C, F and R mean. Written 2026-11-06 as a project note; it belongs to the playback part of the system (WP6a), not to the data cleaning or the database schema.

## The problem

Starting stock for each restaurant was planned as "N days of that restaurant's average ingredient usage". If that average is taken over all 90 days of orders, and playback then sends those same 90 days, the starting stock is built from orders that have not happened yet in the replay. That is data leakage: using information from the future.

It also breaks rare ingredients. An ingredient used a few times in 90 days has an average near zero, its starting stock rounds to 0, and it is out of stock from the first order.

## The rule

1. **Warm-up window.** Measure usage on the first W days only. Playback starts at day W, so warm-up orders are never replayed.
2. **Floor.** Every (restaurant, item) stock counter starts with at least F portions, including items never seen during warm-up.
3. **Starting stock** = max(F, ceil(average daily warm-up usage x C days of cover)). The same number is the par level.
4. **Restock** back to the par level every R days of replay.
5. **Check before real runs.** Replay the orders against these numbers by plain counting, with no database (the WP6a oracle). Report how many stock counters ever reach zero and how many orders would be refused. Adjust W, C, F and R if stock-outs are far too common or never happen.

Starting values: W = 14 days, C = 7 days, F = 5 portions, R = 7 days.

## To check first

- DHRD ships `orders_sg_train.txt` and `orders_sg_test.txt`, which together equal `orders_sg.txt`. Check whether that split is by time (every test day after every train day). If it is, the train/test split could replace the warm-up window.
- Inputs come from the cleaned data: `orders.csv`, `order_items.csv` (quantity), `product_dish.csv` (multiplier), `dish_ingredients.csv` (portions).

## Wording for the final report

- Phase 2 Section 2.7, step 4 ("Set stock levels"): stock comes from a warm-up window, not the whole dataset, and has a floor.
- Phase 2 Section 2.6 (replay): the replay starts at day W, not day 0.
- One line for the report: "Stock parameters are estimated from the first W days only; the replay uses the remaining days, so no future orders inform the starting stock."
