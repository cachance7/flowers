from flowers.parsers.swgmc import SwgmcParser

# TODO: remove this
file = "./swgmc_2023-03-13.pdf"


def make_parser(file):
    return SwgmcParser(file)


def run(file):
    parser = make_parser(file)
    return parser.parse()


frame = None
if __name__ == "__main__":
    frame = run(file)
