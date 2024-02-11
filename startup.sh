function install_dependencies
{
    sudo apt-get update -y
    sudo apt-get install -y python3-pip
    pip3 install -r ./utils/requirements.txt
}


function main
{
    install_dependencies
}

main "$@"