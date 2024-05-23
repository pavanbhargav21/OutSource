## Question

In a university, your attendance determines whether you will be
allowed to attend your graduation ceremony.
You are not allowed to miss classes for four or more consecutive days.
Your graduation ceremony is on the last day of the academic year,
which is the Nth day.

Your task is to determine the following:
1. The number of ways to attend classes over N days.
2. The probability that you will miss your graduation ceremony.

Represent the solution in the string format as "Answer of (2) / Answer
of (1)", don't actually divide or reduce the fraction to decimal
Test cases:
for 5 days: 14/29
for 10 days: 372/773

Solution Provided:

a. Calculated total number of possible ways to attend the college for 'N' days.
   -> count : (2^N) Represents total number of ways to attend college for N days
   
b. Number of ways to attend classes over 'N' days : 2^N -(ways of missing four or more consecutive days).
   -> possible_ways : Represents total number of ways to attend without 4 or more consecutive days absent
   
c. Probability that you will miss your graduation ceremony.
   -> lastday_abs_ways : Represent total number of ways to not attend Graduation day even if it's eligible to attend
