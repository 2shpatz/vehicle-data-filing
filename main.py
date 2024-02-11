from storage_filer.storage_filer import StorageFiler

def main():
    file_storage = StorageFiler()
    file_storage.start_storage_filer_process()

if __name__ == '__main__':
    main()
