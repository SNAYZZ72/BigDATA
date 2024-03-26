import sys

def parse_input(file):
    for line in file:
        yield line.strip()

def main():
    for line in parse_input(sys.stdin):
        if "Marque" in line:  # Skip header line
            continue

        line = line.replace("\u00a0", " ")
        splitted_line = line.split(",")

        # Gestion colonne marque
        marque = splitted_line[1].split()[0].replace("\"", "")

        # Gestion colonne Malus/Bonus
        malus_bonus = splitted_line[2].replace(" ", "").replace("€1", "").replace("€", "").replace("\"", "")
        if malus_bonus in ["150kW(204ch)", "100kW(136ch)"]:
            continue
        if len(malus_bonus) == 1:
            malus_bonus = "0"

        # Gestion colonne cout energie
        cout = splitted_line[4].split()
        if len(cout) == 2:
            cout = cout[0]
        elif len(cout) == 3:
            cout = cout[0] + cout[1]

        # Gestion colonne Rejet CO2
        rejet = splitted_line[3]

        new_value = f"{malus_bonus}|{rejet}|{cout}"
        print(f"{marque}\t{new_value}")

if __name__ == "__main__":
    main()