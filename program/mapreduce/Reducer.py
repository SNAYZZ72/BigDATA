import sys
from collections import defaultdict

def parse_input(file):
    for line in file:
        yield line.strip()

def main():
    data = defaultdict(list)
    
    for line in parse_input(sys.stdin):
        key, value = line.split('\t', 1)
        data[key].append(value)

    for key, values in data.items():
        sommeBonus_Malus, sommeRejet, sommeCout, count = 0, 0, 0, 0
        
        for value in values:
            malus_bonus, rejet, cout = map(int, value.split('|'))
            sommeBonus_Malus += malus_bonus
            sommeRejet += rejet
            sommeCout += cout
            count += 1

        moyenneMalus_Bonus = sommeBonus_Malus // count
        moyenneRejet = sommeRejet // count
        moyenneCout = sommeCout // count

        print(f"{key}\t{moyenneMalus_Bonus}\t{moyenneRejet}\t{moyenneCout}")

if __name__ == "__main__":
    main()