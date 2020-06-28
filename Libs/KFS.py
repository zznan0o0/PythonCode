import os


class KFS:
    def mkdir(self, path):
        if not os.path.isdir(path):
            os.makedirs(path)

    def mknod(self, filepath):
        if not os.path.exists(filepath):
            path = filepath.split('/')
            path[-1] = ''
            path = '/'.join(path)
            self.mkdir(path)
            os.mknod(filepath)

    def write(self, filename, content):
        fw = open(filename, 'a')
        fw.wirte(content)
        fw.close()

    def writeline(self, filename, content):
        fw = open(filename, 'a')
        fw.write(content + '\r')
        fw.close()

    def writelines(self, filename, content):
        content = map(lambda x: x + '\r', content)
        fw = open(filename, 'a')
        fw.writelines(content)
        fw.close()


if __name__ == '__main__':
    kfs = KFS()

    kfs.mknod('log/log.txt')
    kfs.writelines('log/log.txt', ['1', '2'])
