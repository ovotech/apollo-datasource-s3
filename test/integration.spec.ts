import { DataSourceConfig } from 'apollo-datasource';
import { InMemoryLRUCache } from 'apollo-server-caching';
import { S3 } from 'aws-sdk';
import { S3DataSource } from '../src';

class TestS3DataSource extends S3DataSource {
  async get(name: string) {
    return await this.getObject({ Bucket: 'bucket', Key: name });
  }

  async put(name: string, data: any) {
    return await this.putObjectJson(data, { Bucket: 'bucket', Key: name });
  }

  async getJson(name: string) {
    return await this.getObjectJson({ Bucket: 'bucket', Key: name });
  }

  async putJson(name: string, data: {}) {
    return await this.putObjectJson(data, { Bucket: 'bucket', Key: name });
  }

  async delete(name: string) {
    return await this.deleteObject({ Bucket: 'bucket', Key: name });
  }

  async list() {
    return await this.listObjects({ Bucket: 'bucket' });
  }
}

const s3 = new S3({
  endpoint: `http://localhost:4572`,
  sslEnabled: false,
  s3ForcePathStyle: true,
  secretAccessKey: '123',
  accessKeyId: 'asd',
});

const config: DataSourceConfig<{}> = { cache: new InMemoryLRUCache(), context: {} };

const dataSource = new TestS3DataSource(s3);

dataSource.initialize(config);

const deleteBucketContents = async (name: string) => {
  const { Contents } = await s3.listObjects({ Bucket: name }).promise();
  if (Contents.length) {
    const params: S3.Types.DeleteObjectsRequest = {
      Bucket: name,
      Delete: { Objects: Contents.map(item => ({ Key: item.Key })) },
    };
    await s3.deleteObjects(params).promise();
  }
};

describe('Integration test', () => {
  beforeAll(async () => {
    const { Buckets } = await s3.listBuckets().promise();
    if (Buckets.find(item => item.Name === 'bucket')) {
      await deleteBucketContents('bucket');
      await s3.deleteBucket({ Bucket: 'bucket' }).promise();
    }
    await s3.createBucket({ Bucket: 'bucket' }).promise();
  });

  it('Should get and cache data', async () => {
    // Error on missing key
    await expect(dataSource.get('test')).rejects.toEqual(new Error('The specified key does not exist.'));

    // Put empty key
    await expect(dataSource.put('test', undefined)).resolves.toEqual(undefined);

    // Check if object is listed
    await expect(dataSource.list()).resolves.toEqual([expect.objectContaining({ Key: 'test' })]);

    // Check if data in key is "empty"
    await expect(dataSource.get('test')).resolves.toEqual(new Buffer([]));

    // Should fail when content is empty
    await expect(dataSource.getJson('test')).rejects.toEqual(new Error('Missing or invalid body'));

    // Delete empty object
    await expect(dataSource.delete('test')).resolves.toEqual(undefined);

    // Check if object is not listed after deletion
    await expect(dataSource.list()).resolves.toEqual([]);

    // Put a json object
    await expect(dataSource.putJson('test', { data: 'test' })).resolves.toEqual(undefined);

    // Retrive the json object
    await expect(dataSource.getJson('test')).resolves.toEqual({ data: 'test' });
  });
});
