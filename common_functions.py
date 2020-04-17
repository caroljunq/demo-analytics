import random
import time

def random_probabilities(min_value, max_value):
    """
    Generate random probabilities based range of interval
    PARAMETER
    MIN_VALUE: int
    MAX_VALUE: int
    PROBABILITIES: list of floats
    """
    delta = max_value - min_value + 1
    probabilities = [random.random() for _ in range(delta)] # random initial probabilities
    sum_probabilities = sum(probabilities)
    
    # https://stackoverflow.com/questions/2640053/getting-n-random-numbers-whose-sum-is-m
    # Just generate N random numbers, compute their sum, divide each one by the sum and multiply by M.
    for i in range(len(probabilities)):
        # generating probabilities equal to  1
        probabilities[i] = probabilities[i]/sum_probabilities
    
    return probabilities
    
# https://stackoverflow.com/questions/553303/generate-a-random-date-between-two-other-dates
def str_time_prop(start, end, format, prop):
    """Get a time at a proportion of a range of two formatted times.

    start and end should be strings specifying times formated in the
    given format (strftime-style), giving an interval [start, end].
    prop specifies how a proportion of the interval to be taken after
    start.  The returned time will be in the specified format.
    """

    stime = time.mktime(time.strptime(start, format))
    etime = time.mktime(time.strptime(end, format))

    ptime = stime + prop * (etime - stime)

    return time.strftime(format, time.localtime(ptime))


def random_date(start, end, prop):
    # Return a random date between the passed interval
    return str_time_prop(start, end, '%Y-%m-%d %H:%M:%S', prop)

# integers
# values_highest_perc = [18,19,20,21,22,23,24,25] ex: highest_perc = 50% between 18-25 years old, delta = 8 elements, 
# 6,25% each, or 0.0625
def random_prob_sum(index_start, index_end, values_highest_perc, highest_perc):
    values = list(range(index_start, index_end + 1))
    elem_perc = highest_perc/len(values_highest_perc) # percent value per elements in the values_highest_perc
    other_perc = 1 - highest_perc # percentvalue of the rest of the values
    probabilities = [random.random() for _ in range(len(values))] # random initial probabilities
    sum_probabilities = sum(probabilities)
    
    # https://stackoverflow.com/questions/2640053/getting-n-random-numbers-whose-sum-is-m
    # Just generate N random numbers, compute their sum, divide each one by the sum and multiply by M.
    for i in range(len(probabilities)):
        # generating probabilities equal to other_perc (1 - highest_perc)
        probabilities[i] = (probabilities[i]/sum_probabilities) * other_perc
        
    for i in range(len(values)):
        if values[i] in values_highest_perc:
            probabilities[i] = elem_perc
            
    # fixing acumulate error       
    err_corr_per_elem = (1 - sum(probabilities))/len(values)
    # distributing errors to all elements
    probabilities = [elem + err_corr_per_elem for elem in probabilities]

    return probabilities