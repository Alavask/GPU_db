using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using ILGPU;

namespace Gpu_db
{
    enum Type
    {
        SELECT,
        INSERT,
        NONE
    }

    enum State
    {
        Type,
        Table,
        Fields,
        Values
    }
    class Program
    {
        public static void Main(string[] args)
        {
            String input = "INSERT INTO tt (a b c) VALUES (1 2 3)";
            String[] words = input.Split();
            bool correct = true;
            string dir="";
            List<String> fields = new List<string>();
            List<String> values = new List<string>();
            List<String> tables = new List<string>();
            State state = State.Type;
            Type type =Type.NONE;

            for (var i=0; i<words.Length; i++)
            {
                if (!correct)
                {
                    break;
                }
                if (i == 0 && state==State.Type)
                {
                    switch (words[i])
                    {
                        case "SELECT":
                            type = Type.SELECT;
                            state = State.Table;
                            continue;
                        case "INSERT":
                            if (words.Length > 1)
                            {
                                if (words[1] == "INTO")
                                {
                                    type = Type.INSERT;
                                    state = State.Table;
                                    i = 1;
                                    continue;
                                }
                                else
                                {
                                    correct = false;
                                }
                            }
                            else
                            {
                                correct = false;
                            }
                            break;
                        default:
                            correct = false;
                            break;
                    }
                }
                switch (type)
                {
                    case Type.INSERT:
                        switch (state)
                        {
                            case State.Table:
                                if (!Directory.Exists(words[i]))
                                {
                                    Directory.CreateDirectory(words[i]);
                                }
                                dir = words[i] + "\\";
                                state = State.Fields;
                                continue;
                            case State.Fields:
                                if (words[i][0] == '(' || fields.Count()>0)
                                {
                                    if (words[i][0] == '(')
                                    {
                                        fields.Add(words[i].Substring(1));
                                    }
                                    else
                                    {
                                        if (words[i][words[i].Length - 1] == ')')
                                        {
                                            fields.Add(words[i].Substring(0, words[i].Length - 1));
                                            state = State.Type;
                                            break;
                                        }
                                        fields.Add(words[i]);
                                    }
                                }
                                else
                                {
                                    correct = false;
                                }
                                continue;
                            case State.Type:
                                if (words[i] == "VALUES")
                                {
                                    state = State.Values;
                                }
                                else
                                {
                                    correct = false;
                                }
                                continue;
                            case State.Values:
                                if (words[i][0] == '(' || values.Count() > 0)
                                {
                                    if (words[i][0] == '(')
                                    {
                                        values.Add(words[i].Substring(1));
                                    }
                                    else
                                    {
                                        if (words[i][words[i].Length - 1] == ')')
                                        {
                                            values.Add(words[i].Substring(0, words[i].Length - 1));
                                            state = State.Type;
                                            if(words.Length>i+1 || values.Count!=fields.Count)
                                            {
                                                correct = false;
                                            }
                                            break;
                                        }
                                        values.Add(words[i]);
                                    }
                                }
                                else
                                {
                                    correct = false;
                                }
                                break;
                        }
                        break;
                    case Type.SELECT:
                        switch (state)
                        {
                            case State.Table:

                                break;
                        }
                        break;
                    default:
                        correct = false;
                        break;
                }
            }

            if (correct)
            {
                switch (type)
                {
                    case Type.INSERT:
                        for(int i = 0; i < fields.Count; i++)
                        {
                            Dictionary<String, int> map = new Dictionary<String, int>();
                            int id = 0;
                            using (FileStream fstream = new FileStream(dir + fields[i] + ".map", FileMode.OpenOrCreate))
                            {
                                byte[] array = new byte[fstream.Length];
                                fstream.Read(array, 0, array.Length);
                                String textFromFile = System.Text.Encoding.Default.GetString(array);
                                String[] names = textFromFile.Split(';');
                                int iter = 0;
                                foreach (var name in names)
                                {
                                    map.Add(name, iter++);
                                }
                                if (!map.TryGetValue(values[i],out id))
                                {
                                    id = iter + 1;
                                    byte[] output = System.Text.Encoding.Default.GetBytes(values[i]+";");
                                    fstream.Seek(0, SeekOrigin.End);
                                    fstream.Write(output, 0, output.Length);
                                }
                            }
                            using (FileStream fstream = new FileStream(dir + fields[i] + ".vec", FileMode.Append))
                            {
                                byte[] output = System.Text.Encoding.Default.GetBytes(id.ToString()+";");
                                fstream.Write(output, 0, output.Length);
                            }

                        }
                        break;
                    case Type.SELECT:
                        break;
                }
            }



            if (correct)
            {
                using (var context = new Context())
                {
                    using (var accelerator = new ILGPU.Runtime.Cuda.CudaAccelerator(context))
                    {
                        var myKernel = accelerator.LoadAutoGroupedKernel <Index1, ArrayView<int>, ArrayView<int>, int>(EqualsKernel);

                        using (var buffer = accelerator.Allocate<int>(1024))
                        {
                            myKernel(buffer.Length, buffer.View, 42);

                            accelerator.Synchronize();

                            var data = buffer.GetAsArray();
                        }
                    }
                }
            }

            if (!correct)
            {
                Console.WriteLine("Cant read expression");
            }
            else
            {

            }
            Console.Read();
        }


        static void EqualsKernel(Index1 index, ArrayView<int> dataViewIn, ArrayView<int> dataViewOut, int constant)
        {
            if (dataViewIn[index] == constant)
            {
                dataViewOut[index] = dataViewIn[index];
            }
            else
            {
                dataViewOut[index] = -1;
            }
        }

        static void VecEqualKernel(Index2 index, ArrayView<int> dataViewIn1, ArrayView<int> dataViewIn2, ArrayView<int> dataViewOut)
        {
            if (dataViewIn1[index.X] == dataViewIn1[index.Y])
            {
                dataViewOut.push_back((index.X^+index.Y)); //TODO: dynamic allocation and compression
            }
        }

        static void JoinKernel(Index1 index, ArrayView<int> dataViewInter, ArrayView<int> dataViewAdd1, ArrayView<int> dataViewAdd2, ArrayView<int> dataViewOut)
        {
            //compiles vector from 2 add and intersect
        }
    }
}