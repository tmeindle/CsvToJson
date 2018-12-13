using CommandLine;
using System;
using System.Collections;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Linq;

namespace CsvToJson
{

    class Options
    {
        [CommandLine.Option('i', "input", Required = true, HelpText = "The input delimited file")]
        public string InputFile  { get; set; }

        [CommandLine.Option('o', "output", Required = true, HelpText = "The output json file")]
        public string OutputFile { get; set; }

        [CommandLine.Option('d', "delimiter", Default = ",", Required = false, HelpText = "The delimiter")]
        public string Delimiter { get; set; }

        [CommandLine.Option('q', "qualifier", Default = "\"", Required = false, HelpText = "The qualifier")]
        public string Qualifier { get; set; }

        [CommandLine.Option("trim", Default = true, Required = false, HelpText = "Trim whitespace from fields")]
        public bool Trim { get; set; }

    }

    public class Program
    {
        static void Main(string[] args)
        {
            CommandLine.Parser.Default.ParseArguments<Options>(args).MapResult((Options opts) => RunAndExit(opts), errs => 1);
        }

        static int RunAndExit(Options options)
        {
            var delim = ParseString(options.Delimiter)[0];
            var qualif = options.Qualifier == "\"" ? '\"' : ParseString(options.Qualifier)[0];

            var reader = new DelimitedReader(options.InputFile, delim, qualif);
            using (var writer = new StreamWriter(options.OutputFile))
            {
                writer.WriteLine("{");
                var header = ((IEnumerable<IEnumerable<string>>)reader).First().ToArray();
                if (options.Trim)
                {
                    for (int i = 0; i < header.Length; i++)
                    {
                        header[i] = header[i].Trim();
                    }
                }

                bool first = true;
                foreach (var line in reader.Select(a => a.ToArray()).Skip(1))
                {
                    if (!first)
                    {
                        writer.WriteLine(",");
                    }
                    first = false;

                    if (line.Length != header.Length)
                    {
                        throw new ApplicationException("line length does not match header length");
                    }

                    var lineDict = new Dictionary<string, string>(line.Length);
                    for (int i = 0; i < line.Length; i++)
                    {
                        if (options.Trim)
                        {
                            lineDict.Add(header[i], options.Trim?line[i].Trim():line[i]);
                        }
                    }


                    writer.Write(Newtonsoft.Json.JsonConvert.SerializeObject(lineDict, new Newtonsoft.Json.JsonSerializerSettings() { }));

                }
                writer.WriteLine();
                writer.WriteLine("}");
            }

            return 0;
        }

        public static string ParseString(string txt)
        {
            var provider = new Microsoft.CSharp.CSharpCodeProvider();
            var prms = new System.CodeDom.Compiler.CompilerParameters();
            prms.GenerateExecutable = false;
            prms.GenerateInMemory = true;
            var results = provider.CompileAssemblyFromSource(prms, @"
                                namespace tmp
                                {
                                    public class tmpClass
                                    {
                                        public static string GetValue()
                                        {
                                            return " + "\"" + txt + "\"" + @";
                                        }
                                    }
                                }");
            System.Reflection.Assembly ass = results.CompiledAssembly;
            var method = ass.GetType("tmp.tmpClass").GetMethod("GetValue");
            return method.Invoke(null, null) as string;
        }
    }

    public class DelimitedReader : IEnumerable<IEnumerable<string>>
    {
        private const int DEFAULT_CHUNK_SIZE = 128;
        private const char DEFAULT_ESCAPE_CHAR = '"';
        private const char DEFAULT_SEPARATOR_CHAR = ',';

        private readonly char[] m_buffer;
        private readonly Encoding m_encoding;
        private readonly char m_escapeChar;
        private readonly string m_fileName;
        private readonly char m_separatorChar;

        public char[] Buffer
        {
            get
            {
                return m_buffer;
            }
        }
        public Encoding Encoding
        {
            get
            {
                return m_encoding;
            }
        }
        public char EscapeChar
        {
            get
            {
                return m_escapeChar;
            }
        }
        public string FileName
        {
            get
            {
                return m_fileName;
            }
        }
        public char SeparatorChar
        {
            get
            {
                return m_separatorChar;
            }
        }

        public DelimitedReader(string fileName, char separatorChar = DEFAULT_SEPARATOR_CHAR, char escapeChar = DEFAULT_ESCAPE_CHAR, Encoding encoding = null, int bufferSize = DEFAULT_CHUNK_SIZE)
        {
            m_buffer = new char[bufferSize];
            m_encoding = (encoding ?? Encoding.UTF8);
            m_escapeChar = escapeChar;
            m_fileName = fileName;
            m_separatorChar = separatorChar;
        }

        public IEnumerator<IEnumerable<string>> GetEnumerator()
        {
            return ReadFields().GetEnumerator();
        }
        IEnumerator IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
        }
        IEnumerable<IEnumerable<string>> ReadFields()
        {
            return ReadFields(ReadAllChunks(FileName, Encoding, Buffer), SeparatorChar, EscapeChar);
        }

        public static DelimitedReader Create(string fileName, char separatorChar = DEFAULT_SEPARATOR_CHAR, char escapeChar = DEFAULT_ESCAPE_CHAR, Encoding encoding = null, int bufferSize = DEFAULT_CHUNK_SIZE)
        {
            return new DelimitedReader(fileName, separatorChar, escapeChar, encoding, bufferSize);
        }
        public static IEnumerable<char[]> ReadAllChunks(TextReader reader, char[] buffer)
        {
            var count = buffer.Length;
            var numBytesRead = 0;

            while ((numBytesRead = reader.ReadBlock(buffer, 0, count)) == count)
            {
                yield return buffer;
            }

            if (numBytesRead > 0)
            {
                Array.Resize(ref buffer, numBytesRead);

                yield return buffer;
            }
        }
        public static IEnumerable<char[]> ReadAllChunks(string fileName, Encoding encoding, char[] buffer)
        {
            return ReadAllChunks(new StreamReader(new FileStream(fileName, FileMode.Open, FileAccess.Read, FileShare.Read, 4096, FileOptions.SequentialScan), encoding), buffer);
        }
        public static string ReadField(StringBuilder buffer, int offset, int position, char escapeChar)
        {
            if (buffer[offset] == escapeChar)
            {
                if (position - offset != 2)
                {
                    return buffer.ToString(offset + 1, position - offset - 3);
                }
                else
                {
                    return string.Empty;
                }
            }
            else
            {
                return buffer.ToString(offset, position - offset - 1);
            }
        }
        public static IEnumerable<IEnumerable<string>> ReadFields(IEnumerable<char[]> chunks, char separatorChar = DEFAULT_SEPARATOR_CHAR, char escapeChar = DEFAULT_ESCAPE_CHAR)
        {
            var buffer = new StringBuilder();
            var fields = new List<string>();
            var endOfBuffer = 0;
            var escaping = false;
            var offset = 0;
            var position = 0;
            var head0 = '\0';
            var head1 = head0;

            foreach (var chunk in chunks)
            {
                buffer.Append(chunk, 0, chunk.Length);
                endOfBuffer = buffer.Length;

                while (position < endOfBuffer)
                {
                    head1 = head0;

                    if ((head0 = buffer[position++]) == escapeChar)
                    {
                        escaping = !escaping;

                        if ((head0 == escapeChar) && (head1 == escapeChar))
                        {
                            endOfBuffer--;
                            position--;
                            buffer.Remove(position, 1);
                        }
                    }

                    if (!escaping)
                    {
                        if ((head0 == '\n') || (head0 == '\r'))
                        {
                            if ((head1 != '\r') || (head0 == '\r'))
                            {
                                fields.Add(ReadField(buffer, offset, position, escapeChar));

                                yield return fields;

                                buffer.Remove(0, position);
                                endOfBuffer = buffer.Length;
                                fields.Clear();
                                offset = 0;
                                position = 0;
                            }
                            else
                            {
                                offset++;
                            }
                        }
                        else if (head0 == separatorChar)
                        {
                            fields.Add(ReadField(buffer, offset, position, escapeChar));
                            offset = position;
                        }
                    }
                }
            }


            if (buffer.Length > 0)
            {
                fields.Add(buffer.ToString());
            }

            if (fields.Count > 0)
            {
                yield return fields;
            }
        }
    }
}

 
