#This code will give a flattened file to run sparkSQL
import json
# original file was written with pretty-print inside a list
with open("ux_event.json") as jsonfile:
    js = json.load(jsonfile)      

# write a new file with one object per line
with open("flattened_ux.json", 'a') as outfile:
    for d in js:
        json.dump(d, outfile)
        outfile.write('\n')

