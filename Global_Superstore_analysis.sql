-- Data Exploration

select * from gs
limit 20;


select count(distinct(order_id))as order_count,
	   count(distinct(customer_id))as customer_count,
	   count(distinct(product_name))as product_count
from gs	   ;



select distinct(region)from gs ;
select distinct(segment)from gs;
select distinct(category)from gs;



--Date range (1453 days)
select max(order_date)-min(order_date) from gs;


--Total sale (1710971.46), Total profit(288920.44), Average sale per order(1711)
select sum(sales),sum(profit),ROUND(avg(sales)) from gs;



-- Ship modes and their usage
select ship_mode,count(*) from gs 
group by ship_mode
;


-- countries and their orders
select country , count(*) from gs
group by country
order by count(*) desc;


-- most expensive products by unit price
select product_name,(sum(sales)/sum(quantity))as unit_price
from gs
group by product_name
order by unit_price desc;


--cheapest products by unit price 
select product_name,(sum(sales)/sum(quantity))as unit_price
from gs
group by product_name
order by unit_price asc;



--Sales analysis

--yearly sales analysis
select extract(year from order_date)as years,Round(sum(sales))as Sales
from gs
group by extract(year from order_date)
order by extract(year from order_date);


--top best selling products by revenue
select product_name,Round(sum(sales))as revenue
from gs
group by product_name
order by sum(sales) desc
limit 5;



--Sales by region 
select region ,round(sum(sales))as sales
from gs
group by region 
order by sum(sales) desc;



-- sales of top 5 product of each category
with df as(select category,product_name,
sum(sales)as Sales
from gs
group by category,product_name),

a as(select *,
dense_rank() over(partition by category order by Sales desc)as rnk
from df)

select category,product_name,round(sales)
from a
where rnk between 1 and 5;



-- profit margin by category 
select category,(sum(profit)/sum(sales))as profit_margin
from gs
group by category ;



--segment comparision 
select segment,sum(sales)as Total_sale,
sum(profit)as Total_profit,
(sum(profit)/sum(sales))as profit_margin
from gs
group by segment;



--Year-over-Year growth rate calculation
with df as(select extract(year from order_date)as year,*
from gs),

a as(select year,round(sum(sales))as sales
from df
group by year
order by year
),


b as(select *,
lag(sales) over()as previous_sales
from a)

select year,sales,
round((sales-previous_sales)*100/previous_sales)as growth_rate
from b
;



--Quarterly sales breakdown
select extract(year from order_date)as year,
extract(quarter from order_date)as quarter,
round(sum(sales))as sales
from gs
group by extract(year from order_date),
extract(quarter from order_date)
order by extract(year from order_date),
extract(quarter from order_date);



--seasonal patterns in sales
select extract(month from order_date)as month,
to_char(order_date,'Month')as month_name,
round(sum(sales))as monthly_sales
from gs
group by extract(month from order_date),to_char(order_date,'Month')
order by extract(month from order_date);
--high sales can be seen in december and january 
--this can be because of many festivals in this peroid



--Customer analysis


--top 10 customers by sale
select customer_name,round(sum(sales))as total_sales
from gs
group by customer_name
order by sum(sales) desc
limit 10;



--customer retention rate

with monthly as(
select customer_name,
date_trunc('month',order_date)as month
from gs
),


a as(select m1.month as previous_month,
m2.month as current_month,
count(distinct(m1.customer_name))as retained_customers
from monthly m1
join monthly m2 on m1.customer_name=m2.customer_name
and m2.month=m1.month + interval '1 month'
group by m1.month,m2.month),


b as(select month,count(distinct(customer_name))as total_customer
from monthly 
group by month)


select a.previous_month,a.current_month,
b.total_customer as previous_customers,
a.retained_customers,
nullif(round(a.retained_customers*100/b.total_customer,2),0)as retention
from a join b 
on a.previous_month=b.month;



--One-time vs repeat customers analysis

with df as 
(select customer_name,round(sum(sales))as sale,
count(distinct(order_id))as orders,
case when count(distinct(order_id))=1 then 'one-time'
else 'repeat' end as category
from gs
group by customer_name)


select category,sum(sale)as sale,sum(orders)as orders
from df
group by category ;


--Most valuable customers by region

with df as
(select region ,customer_name,
sum(profit)as profit 
from gs
group by region,customer_name),


a as(select * ,
rank() over(partition by region order by profit desc)
from df
order by region 
)


select *
from a 
where rank between 1 and 5;


--Product & Operations 

--Products with highest/lowest profit margins


with df as(select product_name,round(sum(profit)*100/sum(sales))as margin 
from gs
group by product_name)

(select * from df 
order by margin desc
limit 5)
union all
(select * from df 
order by margin 
limit 5)
order by margin desc;


--Products frequently bought together 


with df as(select a.product_id,b.product_id,count(*)
from gs a
join gs b 
on a.order_id=b.order_id
and a.product_id < b.product_id
group by a.product_id,b.product_id)

select * from df
where count>5;



--Discount effectiveness analysis

with df as(select * ,
case when discount=0 then 'no'
when discount >0.00 and discount<=.10 then 'below 10%'
when discount>.10 and discount<=.20 then 'below 20%'
else 'high'
end as discount_category
from gs
)

select discount_category,round(sum(sales))as sale,round(sum(profit))as profit,
round((sum(profit)*100/(sum(sales))))as margin
from df
group by discount_category;



--Shipping cost analysis by mode and region
select region,ship_mode,
round(avg(shipping_cost))as avg_ship_cost
from gs
group by region ,ship_mode
order by region;



--Delivery time analysis and efficiency

select ship_mode,
ROUND(
avg(ship_date::date - order_date::date),2
) as avg_shipping_days
from gs
group by ship_mode;


--Most cost-effective shipping methods

select ship_mode,
sum(shipping_cost)as total_shipping 
,sum(profit)as profit,
round(sum(profit)*100/nullif(sum(shipping_cost),0))as profit_per_per_ship
from gs
group by ship_mode
;



--Moving averages 

with df as
(select order_date,sum(sales)as total_sale
from gs
group by order_date
order by order_date)
    
select * ,
avg(total_sale)
over(order by order_date rows between 6 preceding and current row )as
seven_day_move_avg
from df
;


-- Project Summary:

-- Analyzed Global Superstore sales data to identify profit drivers 
-- and loss contributors.

-- Found that a small % of customers and products contribute the majority
-- of profit.

-- High-value orders with heavy discounts and shipping costs frequently
-- resulted in losses.

-- Standard shipping and high-priority orders dominated, especially in Western
-- Europe.

-- Insights highlight the need for discount control, customer segmentation,
-- and cost-aware fulfillment strategies.





