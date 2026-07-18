Customer Data Anomalies SQL Notes

     - Duplicate customer records were detected for the  customer_id + full_name  combination, including entries such as  (1, 'Alice Smith') .
    - Duplicate customer contact records were also found for the  customer_id + full_name + phone  combination, including  (2, 'Bob Jones', '555-987-6543') .
    - Two records were identified with missing  email  values, and two additional records were found with missing  phone  values, indicating incomplete customer contact data.
    - The  phone  column contains phone numbers in multiple inconsistent formats (for example, some with country codes, some with dashes, and some with only digits).

 Order Data Anomalies SQL Notes
 
    - Two records were identified with missing  currency  values (order_id 107 and 116), and one additional record were found with missing  order_date  values (order_id 117).
     - two records were identified with negative total_amount order values (order_id 103 with -50 and order_id 113 with -100)
    - There is a mismatch in  customer_id  values between the  customers  table and the  ==orders==  table for order numbers 106 and 118
    
  
