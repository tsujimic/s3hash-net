using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Framework
{
    public interface IReaderCloser
    {
        long Length { get; set; }
        int Read(byte[] buffer, int index, int count);
        void Close();
    }
}
