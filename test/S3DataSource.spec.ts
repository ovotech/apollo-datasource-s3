import { DataSourceConfig } from 'apollo-datasource';
import { InMemoryLRUCache } from 'apollo-server-caching';
import { S3 } from 'aws-sdk';
import { S3DataSource } from '../src';

class TestS3DataSource extends S3DataSource {
  async data() {
    return await this.getObjectJson({ Bucket: 'bucket', Key: 'test' });
  }

  async put() {
    return await this.putObjectJson({ data: 'test' }, { Bucket: 'bucket', Key: 'test' });
  }

  async delete() {
    return await this.deleteObject({ Bucket: 'bucket', Key: 'test' });
  }

  async list() {
    return await this.listObjects({ Bucket: 'bucket' });
  }

  internalS3() {
    return this.s3;
  }
}

const s3 = {
  getObject: jest.fn(),
  putObject: jest.fn(),
  deleteObject: jest.fn(),
  listObjects: jest.fn(),
};

let dataSource: TestS3DataSource;

describe('S3Cache', () => {
  beforeEach(() => {
    const config: DataSourceConfig<{}> = {
      cache: new InMemoryLRUCache(),
      context: {},
    };

    s3.getObject.mockClear();
    s3.putObject.mockClear();
    s3.deleteObject.mockClear();
    s3.listObjects.mockClear();

    const s3mock: any = {
      config: { region: 'mock' },
      getObject: () => ({ promise: s3.getObject }),
      putObject: () => ({ promise: s3.putObject }),
      deleteObject: () => ({ promise: s3.deleteObject }),
      listObjects: () => ({ promise: s3.listObjects }),
    };

    dataSource = new TestS3DataSource(s3mock);
    dataSource.initialize(config);
  });

  it('Should instantiate s3 object by default', () => {
    const normalDataSource = new TestS3DataSource();

    expect(normalDataSource.internalS3().config).toEqual(new S3().config);
  });

  it('Should get and cache data', async () => {
    s3.getObject
      .mockResolvedValueOnce({ Body: undefined })
      .mockRejectedValueOnce({ message: 'Message' })
      .mockResolvedValueOnce({ Body: '{"test":"data"}' } as S3.Types.GetObjectOutput);

    await expect(dataSource.data()).rejects.toEqual(new Error('Missing or invalid body'));
    await expect(dataSource.data()).rejects.toEqual(new Error('Message'));
    await expect(dataSource.data()).resolves.toEqual({ test: 'data' });

    // Return data from cache
    await expect(dataSource.data()).resolves.toEqual({ test: 'data' });

    expect(s3.getObject).toHaveBeenCalledTimes(3);
  });

  it('Should put and evict cache data', async () => {
    s3.getObject
      .mockResolvedValueOnce({ Body: '{"test":"data"}' } as S3.Types.GetObjectOutput)
      .mockResolvedValueOnce({ Body: '{"test":"data2"}' } as S3.Types.GetObjectOutput);

    s3.putObject.mockRejectedValueOnce({ message: 'Message' }).mockResolvedValueOnce({});

    await expect(dataSource.put()).rejects.toEqual(new Error('Message'));
    await expect(dataSource.data()).resolves.toEqual({ test: 'data' });
    await expect(dataSource.put()).resolves.toEqual(undefined);
    await expect(dataSource.data()).resolves.toEqual({ test: 'data2' });

    // Return data from cache
    await expect(dataSource.data()).resolves.toEqual({ test: 'data2' });

    expect(s3.getObject).toHaveBeenCalledTimes(2);
    expect(s3.putObject).toHaveBeenCalledTimes(2);
  });

  it('Should delete and evict cache data', async () => {
    s3.getObject
      .mockResolvedValueOnce({ Body: '{"test":"data"}' } as S3.Types.GetObjectOutput)
      .mockResolvedValueOnce({ Body: '{"test":"data2"}' } as S3.Types.GetObjectOutput);

    s3.deleteObject.mockRejectedValueOnce({ message: 'Message' }).mockResolvedValueOnce({});

    await expect(dataSource.delete()).rejects.toEqual(new Error('Message'));
    await expect(dataSource.data()).resolves.toEqual({ test: 'data' });
    await expect(dataSource.delete()).resolves.toEqual(undefined);
    await expect(dataSource.data()).resolves.toEqual({ test: 'data2' });
  });

  it('Should list bucket', async () => {
    s3.listObjects
      .mockRejectedValueOnce({ message: 'Message' })
      .mockResolvedValueOnce({ Contents: [{ Key: 'test' }, { Key: 'test2' }] } as S3.Types.ListObjectsOutput);

    await expect(dataSource.list()).rejects.toEqual(new Error('Message'));
    await expect(dataSource.list()).resolves.toEqual([{ Key: 'test' }, { Key: 'test2' }]);
  });
});
