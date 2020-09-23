using ILGPU;
using ILGPU.Runtime;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using ILGPU.Algorithms;
using ILGPU.Algorithms.ScanReduceOperations;

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
            int max = 100000000;
            

            String input = "INSERT INTO tt (a b c) VALUES ("+ GenerateString(3)+" "+ GenerateString(4) + " "+ GenerateString(5) + ")"; //INSERT INTO tt (a b c) VALUES (1 2 3)
            String[] words = input.Split();
            bool correct = true;
            string dir = "";
            List<String> fields = new List<string>();
            List<String> values = new List<string>();
            List<String> tables = new List<string>();
            State state = State.Type;
            Type type = Type.NONE;

            for (var i = 0; i < words.Length; i++)
            {
                if (!correct)
                {
                    break;
                }
                if (i == 0 && state == State.Type)
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
                                if (words[i][0] == '(' || fields.Count() > 0)
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
                                            if (words.Length > i + 1 || values.Count != fields.Count)
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
                        for (int i = 0; i < fields.Count; i++)
                        {
                            Dictionary<String, int> map = new Dictionary<String, int>();
                            int id = 0;
                            using (FileStream fstream = new FileStream(dir + fields[i] + ".map", FileMode.OpenOrCreate))
                            {
                                byte[] array = new byte[fstream.Length];
                                fstream.Read(array, 0, array.Length);
                                String textFromFile = System.Text.Encoding.Default.GetString(array);
                                String[] names = textFromFile.Split(';');
                                names = names.Where(val => val != "").ToArray();
                                int iter = 0;
                                foreach (var name in names)
                                {
                                    map.Add(name, iter++);
                                }
                                if (!map.TryGetValue(values[i], out id))
                                {
                                    id = iter + 1;
                                    byte[] output = System.Text.Encoding.Default.GetBytes(values[i] + ";");
                                    fstream.Seek(0, SeekOrigin.End);
                                    fstream.Write(output, 0, output.Length);
                                }
                            }
                            using (FileStream fstream = new FileStream(dir + fields[i] + ".vec", FileMode.Append))
                            {
                                byte[] output = System.Text.Encoding.Default.GetBytes(id.ToString() + ";");
                                fstream.Write(output, 0, output.Length);
                            }

                        }
                        break;
                    case Type.SELECT:
                        break;
                }
            }


            //%%%%%%%%%%%%%%%%%%%%
            //
            //START TIME FROM HERE
            //
            //%%%%%%%%%%%%%%%%%%%%

            int[] c = new int[max];

            using (FileStream fstr = new FileStream(dir + "a" + ".vec", FileMode.Open))
            {
                byte[] array = new byte[fstr.Length];
                fstr.Read(array, 0, array.Length);
                String textFromFile = System.Text.Encoding.Default.GetString(array);
                String[] names = textFromFile.Split(';');
                int[] ids = new int[names.Length-1];
                for (int o = 0; o < names.Length-1; o++) {
                    ids[o] = Convert.ToInt32(names[o]);
                }
                for (int i = 0; i < max; i++)
                {
                    c[i] = ids[i%ids.Length];
                }

            }

            String[] result;

            if (true)
            {
                using (var context = new Context())
                {
                    using (var accelerator = new ILGPU.Runtime.Cuda.CudaAccelerator(context))
                    {
                        var kernel = accelerator.LoadAutoGroupedStreamKernel < Index1, ArrayView<int>, ArrayView<int>, int>(LessKernel);
                        var buffer = accelerator.Allocate<int>(max);
                        var buffer2 = accelerator.Allocate<int>(max);
                        
                        buffer.CopyFrom(c, 0,0, max);

                        kernel(buffer.Length, buffer.View, buffer2.View, 30000);

                        var data = buffer.GetAsArray();
                        var data2 = buffer2.GetAsArray();
                        data2 = data2.Where(x => (x!=-1)).ToArray();
                        result = new String[data2.Length];

                        using (FileStream fstr = new FileStream(dir + "a" + ".map", FileMode.Open))
                        {
                            byte[] array = new byte[fstr.Length];
                            fstr.Read(array, 0, array.Length);
                            String textFromFile = System.Text.Encoding.Default.GetString(array);
                            String[] names = textFromFile.Split(';');
                            for(int jj = 0; jj < data2.Length; jj++)
                            {
                                result[jj] = names[data2[jj]];
                            }
                        }
                    }
                }
            }


            //%%%%%%%%%%%%%%%%%%%%
            //
            //sTOP TIME HERE
            //
            //%%%%%%%%%%%%%%%%%%%%

            if (!true)
            {
                Console.WriteLine("Cant read expression");
            }
            else
            {

            }
            Console.Read();
        }

        //void QueryToTree(List<String> words, out List<Node> tree) //преобразуем последовательность лексем в дерево выражений с адресацией по индексу
        //{
        //    tree = new List<Node>();
        //    tree.Add(new Node("()"));
        //    tree[0].AddArg(-1); //начальная нода
        //    int pos = 0;
        //    foreach (var word in words)
        //    {
        //        if (word == "(")
        //        {
        //            tree.Add(new Node("()"));
        //            tree[pos].AddArg(tree.Count() - 1);
        //            tree[tree.Count() - 1].AddArg(pos);
        //            pos = tree.Count() - 1;
        //        }
        //        else if (word == ")")
        //        {
        //            while (tree[pos].GetType() != "()")
        //            {
        //                pos = tree[pos].GetArgs()[0];
        //            }
        //        }
        //        else if (word > tree[pos].GetType()) //нужна функция-компаратор, выдающая: константа\атрибут > () > операция 1 > ... > операция n
        //        {
        //            tree.Add(new Node(word)); //добавляем новую ноду, на pos - предыдущая нода
        //            tree[pos].AddArg(tree.Count() - 1); //потомок предыдущей - текущая нода
        //            tree[tree.Count() - 1].AddArg(pos); //родитель текущей ноды - предыдущая нода
        //            pos = tree.Count() - 1; //теперь работаем с новой нодой
        //        }
        //        else if (!(word > tree[pos].GetType()))
        //        {
        //            tree.Add(new Node(word)); //добавляем новую ноду, на pos - предыдущая нода
        //            tree[tree.Count() - 1].AddArg(tree[pos].GetArgs()[0]); //ее родитель - родитель предыдущей ноды
        //            tree[pos].GetArgs()[0] = tree.Count() - 1; //родитель предыдущей ноды - текущая нода
        //            tree[tree.Count() - 1].AddArg(pos); // ее потомок - предыдущая нода
        //            pos = tree.Count() - 1; //теперь работаем с новой нодой
        //        }
        //    }

        //}

        //void TreeToList(ref List<Node> tree, //преобразуем дерево выражений в последовательность выполнения
        //    out List<int> ids) //на выходе список ид в дереве, читая который в обратном порядке получим последовательное выполнение выражения без лишнего использования данных
        //{
        //    ids = new List<int>();
        //    ids.Add(0);
        //    int i = 0;
        //    while (i < ids.Count()) //обход в ширину
        //    {
        //        if (tree[ids[i]].GetArgs().Count() > 1)
        //        {
        //            for (int j = 1; j < tree[ids[i]].GetArgs().Count(); j++)
        //            {
        //                ids.Add(tree[ids[i]].GetArgs()[j]);
        //            }
        //        }
        //        i++;
        //    }
        //    List<int> leafs = new List<int>(); //список листьев
        //    List<int> replace = new List<int>(); //список замен
        //    foreach (var id in ids)
        //    {
        //        if (tree[id].GetArgs().Count() == 1) //если лист
        //        {
        //            for (int j = 0; j < leafs.Count(); j++) //проставляем максимально возможную последнюю замену
        //            {
        //                if (tree[leafs[j]].GetType() == tree[id].GetType())
        //                {
        //                    replace[j] = id;
        //                    break;
        //                }
        //            }
        //            leafs.Add(id);
        //            replace.Add(id);
        //        }
        //    }
        //    for (int j = 0; j < leafs.Count(); j++)
        //    {
        //        if (leafs[j] != replace[j]) //если есть чем заменить
        //        {
        //            for(int z=0;z<tree[tree[leafs[j]].GetArgs()[0]].GetArgs().Count();z++) //берем этот элемент, берем его родителя, находим в аргументах элемент и заменяем его на новый
        //            {
        //                if (tree[tree[leafs[j]].GetArgs()[0]].GetArgs()[z] == leafs[j])
        //                {
        //                    tree[tree[leafs[j]].GetArgs()[0]].GetArgs()[z] = replace[j];
        //                }
        //            }
        //        }
        //    }
        //    for (int j = 0; j < leafs.Count(); j++)
        //    {
        //        if (leafs[j] != replace[j]) //если есть чем заменить
        //        {
        //            ids.Remove(leafs[j]); //то удаляем его из последовательности выполнения поскольку больше на него никто не ссылается
        //        }
        //    }
        //}

        static void EqualsKernel(Index1 ind, ArrayView<int> dataViewIn, ArrayView<int> dataViewOut, int constant)
        {
            var globalIndex = Grid.GlobalIndex.X;
            if (dataViewIn[ind] == constant)
            {
                dataViewOut[ind] = dataViewIn[ind];
            }
            else
            {
                dataViewOut[ind] = -1;
            }
        }

        static void LessKernel(Index1 ind, ArrayView<int> dataViewIn, ArrayView<int> dataViewOut, int constant)
        {
            var globalIndex = Grid.GlobalIndex.X;
            if (dataViewIn[ind] < constant)
            {
                dataViewOut[ind] = dataViewIn[ind];
            }
            else
            {
                dataViewOut[ind] = -1;
            }
        }

        public static string GenerateString(int length)
        {
            string result = string.Empty;
            Random random = new Random((int)DateTime.Now.Ticks);
            List<string> characters = new List<string>() { };
            for (int i = 48; i < 58; i++)
            {
                characters.Add(((char)i).ToString());
            }
            for (int i = 65; i < 91; i++)
            {
                characters.Add(((char)i).ToString());
            }
            for (int i = 97; i < 123; i++)
            {
                characters.Add(((char)i).ToString());
            }
            for (int i = 0; i < length; i++)
            {
                result += characters[random.Next(0, characters.Count)];
            }
            return result;
        }

        //static void VecEqualKernel(Index2 index, ArrayView<int> dataViewIn1, ArrayView<int> dataViewIn2, ArrayView<int> dataViewOut)
        //{
        //    if (dataViewIn1[index.X] == dataViewIn1[index.Y])
        //    {
        //        dataViewOut.push_back((index.X^+index.Y)); //TODO: dynamic allocation and compression
        //    }
        //}

        //static void JoinKernel(Index1 index, ArrayView<int> dataViewInter, ArrayView<int> dataViewAdd1, ArrayView<int> dataViewAdd2, ArrayView<int> dataViewOut)
        //{
        //    //compiles vector from 2 add and intersect
        //}
    }

    struct Node
    {
        String type;
        List<int> links; //0- prev, else - arguments

        public Node(String _type)
        {
            type = _type;
            links = new List<int>();
        }

        public void AddArg(int arg)
        {
            links.Add(arg);
        }

        public List<int> GetArgs()
        {
            return links;
        }
        public String GetType()
        {
            return type;
        }
    }
}