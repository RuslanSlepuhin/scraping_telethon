
al_set = []
ru_set = []

with open('Al_list.txt', 'r') as file:
    for i in file:
        al_set.append(i.strip())

with open('Ru_list.txt', 'r') as file:
    for i in file:
        ru_set.append(i.replace('\'', '').replace('+', '').replace('#', '').replace(',', '').strip())

al_set = set(al_set)
ru_set = set(ru_set)

t = al_set.symmetric_difference(ru_set)
t2 = ru_set.symmetric_difference(al_set)

pass