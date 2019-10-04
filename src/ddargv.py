import argparse

class ParseArgs:

    def __init__(self, description):
        parser = argparse.ArgumentParser(description = description)

        parser.add_argument("bucket", type = str, nargs = 1,
                        metavar = "bucket_name", default = None,
                        help = "Name of the S3 bucket where the files are stored.")
    
        parser.add_argument("-m", "--method", type = str, nargs = 1,
                        metavar = "algorithm", default = ["checksum"],
                        help = "Method of detecting duplicates. The default \
                        is 'checksum'.")
    
        parser.add_argument("-r", "--region", type = str, nargs = 1,
                        metavar = "region_name", default = ["us-west-2"],
                        help = "Name of the region where the S3 bucket is located. \
                        The default is 'us-west-2'.")
    
        parser.add_argument("-d", "--dir", type = str, nargs = 1,
                        metavar = "directory", default = ["validation"],
                        help = "Name of the directory where the files are located. \
                        The default is 'validation'.")
    
        args = parser.parse_args()
    
        if args.bucket != None: self._bucket_name = args.bucket[0]
        if args.method != None: self._method_name = args.method[0]
        if args.region != None: self._region_name = args.region[0]
        if args.dir != None: self._dir_name = args.dir[0]

    def get_all(self):
        return (self._bucket_name, self._method_name, self._region_name, self._dir_name)

