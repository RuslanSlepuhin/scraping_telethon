import re

text = 'fghdhhsgd jdnjsa <br>dshbcsah</br> dhsh'
text = re.sub(r'<[\W\w\d]{1,7}>', '', text)

print(text)