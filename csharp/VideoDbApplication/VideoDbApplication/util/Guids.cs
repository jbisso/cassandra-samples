using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VideoDbApplication.util
{
    /*
     * Based on code from http://nickberardi.com/creating-a-time-uuid-guid-in-net/
     */
    class Guids
    {
        private static byte[] node = new byte[6];

        // offset to move from 1/1/0001, which is 0-time for .NET, to gregorian 0-time of 10/15/1582
        private static readonly DateTimeOffset GregorianCalendarStart = new DateTimeOffset(1582, 10, 15, 0, 0, 0, TimeSpan.Zero);
        
        // number of bytes in uuid
        private const int ByteArraySize = 16;

        private const byte GuidClockSequenceByte = 8;
        private const byte NodeByte = 10;
        // multiplex variant info
        private const int VariantByte = 8;
        private const int VariantByteMask = 0x3f;
        private const int VariantByteShift = 0x80;
        // multiplex version info
        private const int VersionByte = 7;
        private const int VersionByteMask = 0x0f;
        private const int VersionByteShift = 4;

        public static Guids()
        {
            Random random = new Random();
            random.NextBytes(node);
        }

        public static Guid GenerateTimeBasedGuid()
        {
            return GenerateTimeBasedGuid(DateTime.Now);
        }

        public static Guid GenerateTimeBasedGuid(DateTime dateTime)
        {
            long ticks = dateTime.Ticks - GregorianCalendarStart.Ticks;

            byte[] guid = new byte[ByteArraySize];
            byte[] clockSequenceBytes = BitConverter.GetBytes(Convert.ToInt16(Environment.TickCount % Int16.MaxValue));
            byte[] timestamp = BitConverter.GetBytes(ticks);

            // copy node
            Array.Copy(node, 0, guid, NodeByte, node.Length);

            // copy clock sequence
            Array.Copy(clockSequenceBytes, 0, guid, GuidClockSequenceByte, clockSequenceBytes.Length);

            // copy timestamp
            Array.Copy(timestamp, 0, guid, 0, timestamp.Length);

            // set the variant
            guid[VariantByte] &= (byte)VariantByteMask;
            guid[VariantByte] |= (byte)VariantByteShift;

            // set the version
            guid[VersionByte] &= (byte)VersionByteMask;
            guid[VersionByte] |= (byte)((int) 0x01 << VersionByteShift);

            return new Guid(guid);
        }
    }
}
