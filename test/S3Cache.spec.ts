import { InMemoryLRUCache } from 'apollo-server-caching';
import { expiresToTtl, S3Cache } from '../src';

describe('S3Cache', () => {
  it.each`
    expires                  | result
    ${'2018-02-02 10:00:30'} | ${30}
    ${'2018-02-02 10:20:00'} | ${1200}
    ${'2018-02-02 09:00:00'} | ${0}
  `('Should process expires for %s', ({ expires, result }) => {
    const currentDate = new Date('2018-02-02 10:00:00');
    const expiresDate = new Date(expires);

    const ttl = expiresToTtl(expiresDate, currentDate);

    expect(ttl).toEqual(result);
  });

  it('Should generate correct keys', () => {
    const cache = new InMemoryLRUCache();
    const s3Cache = new S3Cache(cache, { region: 'us-east-1' });

    const full = s3Cache.cacheKey({ Key: 'key', Bucket: 'bucket', VersionId: 'version' });
    const normal = s3Cache.cacheKey({ Key: 'key', Bucket: 'bucket' });

    expect(full).toEqual('us-east-1:bucket:key:version');
    expect(normal).toEqual('us-east-1:bucket:key:');
  });

  it('Should store and retrieve cache', async () => {
    const cache = new InMemoryLRUCache();
    const s3Cache = new S3Cache(cache, { region: 'us-east-1' });

    const noMatch = await s3Cache.get({ Key: 'key', Bucket: 'bucket' });
    await s3Cache.setObject({ Key: 'key', Bucket: 'bucket' }, { Body: 'test' });
    const match = await s3Cache.get({ Key: 'key', Bucket: 'bucket' });

    expect(noMatch).toBe(undefined);
    expect(match).toEqual('test');
  });

  it('Should handle expires', async () => {
    const cache: any = {
      set: jest.fn(),
    };

    const s3Cache = new S3Cache(cache, {
      region: 'us-east-1',
      currentDate: new Date('2018-02-02 10:00:00'),
      defaultTtl: 10,
    });

    const params = { Key: 'key', Bucket: 'bucket' };

    await s3Cache.setObject(params, { Body: 'test' });
    await s3Cache.setObject(params, { Body: 'test', Expires: new Date('2018-02-02 10:00:30') });

    expect(cache.set).toHaveBeenCalledWith('us-east-1:bucket:key:', 'test', { ttl: 10 });
    expect(cache.set).toHaveBeenLastCalledWith('us-east-1:bucket:key:', 'test', { ttl: 30 });
  });

  it('Should use default values', async () => {
    const cache: any = {
      set: jest.fn(),
    };

    const date = new Date();
    date.setDate(date.getDate() + 1);
    const s3Cache = new S3Cache(cache);

    const params = { Key: 'key', Bucket: 'bucket' };

    await s3Cache.setObject(params, { Body: 'test' });
    await s3Cache.setObject(params, { Body: 'test', Expires: date });

    expect(cache.set).toHaveBeenCalledWith(':bucket:key:', 'test', { ttl: 300 });
    expect(cache.set).toHaveBeenLastCalledWith(':bucket:key:', 'test', { ttl: 86400 });
  });
});
