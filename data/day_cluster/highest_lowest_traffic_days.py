arrivals_by_day = []
one_day = []
day_counter = 0

with open('../2008_five_minute_counts.csv') as five_minute_counts:
    for item in five_minute_counts:
        item = int(item.strip())
        one_day.append(item)
        day_counter = day_counter + 1
        if day_counter == 288:
            day_counter = 0
            arrivals_by_day.append(sum(one_day))
            one_day = []

sorted_indexes = sorted(range(len(arrivals_by_day)), key=lambda i: arrivals_by_day[i])

print 'Busiest 10 Days'
for top_index in sorted_indexes[-10:]:
    print 'index=%s, arrivals=%s' % (top_index, arrivals_by_day[top_index])

print '\n'
print 'Least Busy 10 Days'
for bottom_index in sorted_indexes[:10]:
    print 'index=%s, arrivals=%s' % (bottom_index, arrivals_by_day[bottom_index])

