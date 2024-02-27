import zipfile
import sys

# zip format must be ID1-ID2.zip contains ID1_ID2.pdf, Solution.py
def unzip_double(id1, id2, file_list):
    if not f'{id1}_{id2}.pdf' in file_list:
        print('dry part is missing or has an invalid name')
        exit(1)
    print('Success, IDs are: ' + str(id1) + ", " + str(id2))

def unzip_single(id1, file_list):
    if not f'{id1}.pdf' in file_list:
        print('dry part is missing or has an invalid name')
        exit(1)
    print('Success, ID is: ' + str(id1))


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('You must enter the zip name as a command line argument')
        exit(1)
    zip_file = sys.argv[1]
    with zipfile.ZipFile(zip_file, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        if "Solution.py" not in file_list:
            print('Solution.py is missing')
            exit(1)
           
        ids = zip_file.split('.zip')[0].split('-')
        for id in ids:
            if len(id) != 9 or not id.isnumeric():
                print('Invalid ID in zip name')
        if len(ids) == 2:
            unzip_double(ids[0], ids[1], file_list)
            exit(0)
        elif len(ids) == 1:
            unzip_single(ids[0], file_list)
            exit(0)
        else:
            print('Invalid zip name')
            exit(1)
