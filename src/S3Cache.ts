import { KeyValueCache } from 'apollo-server-caching';
import { S3 } from 'aws-sdk';

export const expiresToTtl = (expires: Date, currentDate: Date = new Date()) => {
  const ttl = Math.round((expires.getTime() - currentDate.getTime()) / 1000);
  return ttl > 0 ? ttl : 0;
};

interface S3CacheOptions {
  defaultTtl?: number;
  region?: string;
  currentDate?: Date;
}

interface Params {
  Key: string;
  Bucket: string;
  VersionId?: string;
}

export class S3Cache {
  private options: S3CacheOptions = { defaultTtl: 300 };

  constructor(private keyValueCache: KeyValueCache, options?: S3CacheOptions) {
    this.options = { ...this.options, ...options };
  }

  cacheKey(params: Params) {
    return `${this.options.region || ''}:${params.Bucket}:${params.Key}:${params.VersionId || ''}`;
  }

  async get(params: Params) {
    return await this.keyValueCache.get(this.cacheKey(params));
  }

  async setObject(params: Params, output: S3.Types.GetObjectOutput) {
    const body = String(output.Body);
    const ttl = output.Expires ? expiresToTtl(output.Expires, this.options.currentDate) : this.options.defaultTtl;

    return await this.keyValueCache.set(this.cacheKey(params), body, { ttl });
  }

  async deleteObject(params: Params) {
    return await this.keyValueCache.set(this.cacheKey(params), '', { ttl: 0 });
  }
}
