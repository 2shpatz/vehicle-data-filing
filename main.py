from storage_filer.storage_filer import StorageFiler

def main():
    file_storage = StorageFiler()
    file_storage.collect_events()

if __name__ == '__main__':
    main()
