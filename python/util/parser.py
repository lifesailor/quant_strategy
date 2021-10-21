import argparse


class ArgParser:
    @staticmethod
    def parse_args():
        parser = argparse.ArgumentParser()
        parser.add_argument("--database", type=bool, default=False)
        args = parser.parse_args()
        return args


if __name__ == "__main__":
    args = ArgParser.parse_args()
