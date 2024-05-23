"""Here, based on number of days all possible permutations have been considered
   count : (2^N) Represents total number of ways to attend college for N days
   possible_ways : Represents total number of ways to attend without 4 or more 
                   consecutive days absent
   lastday_abs_ways : Represent total number of ways to not attend Grad day 
                      even If it's eligible to attend
"""


import itertools

def number_of_ways(n):
    count = 0
    possible_ways = 0
    lastday_abs_ways = 0
    val=['P','A'] # total : 2**n ways
    for ter in itertools.product(val, repeat=n):
        val_str = ''.join(ter)
        if 'AAAA' not in val_str:
            possible_ways += 1
            if val_str[-1] == 'A':
                lastday_abs_ways += 1
        count += 1
    print(f"Total ways : {count}")
    print(f"Number of ways to attend classes over {N} days: {possible_ways} & last day absence ways: {lastday_abs_ways}")
    return f"{lastday_abs_ways}/{possible_ways}"

N=5 #Nth day
final=number_of_ways(N)
print(f"For {N} days: {final}")