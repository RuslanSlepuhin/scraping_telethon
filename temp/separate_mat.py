

with open('mat.txt', 'r') as file:
    text = file.read()

text_list = text.split(',')

with open('../filters/mat2.txt', 'a') as file:
    for i in text_list:
        file.write(f'{i.strip()}\n')
