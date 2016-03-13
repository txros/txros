Run tests with:

    trial txros

Run tests and see coverage information with:

    roscd txros
    coverage run `which trial` txros
    coverage report --include 'src/txros/*,src/txros/*/*,src/txros/*/*/*' --omit 'src/txros/test/*,src/txros/test/*/*' -m

You probably need to install coverage first with:

    pip install coverage
    echo 'PATH=${PATH}:~/.local/bin' >> ~/.bashrc
