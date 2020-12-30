#!/usr/bin/env python

import config

def main():
    config.strategy_to_test.run(*config.strategy_args)

if __name__ == '__main__':
    main()
