import glob
'''
This file combines all files in a folder into one.
'''
read_files = glob.glob("./twitter_count/*")

with open("./twitter_count/results.dat", "wb") as outfile:
    for f in read_files:
        with open(f, "rb") as infile:
            outfile.write(infile.read())