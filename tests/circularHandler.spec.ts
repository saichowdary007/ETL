import { CircularReferenceHandler } from '../src/core/CircularReferenceHandler';
import { Readable } from 'stream';

describe('CircularReferenceHandler', () => {
  it('should remove self circular references', (done) => {
    const handler = new CircularReferenceHandler().createHandler();
    const records = [
      { id: 1, parent_id: 1 },
      { id: 2, parent_id: 1 }
    ];
    const out: any[] = [];
    Readable.from(records)
      .pipe(handler)
      .on('data', (r) => out.push(r))
      .on('end', () => {
        expect(out[0].parent_id).toBeNull();
        done();
      });
  });
}); 