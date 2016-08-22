using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace Pomelo.EntityFrameworkCore.MyCat.Proxy
{
    public class MyCatDataNode
    {
        public MyCatDatabaseHost Slave { get; set; }
        public MyCatDatabaseHost Master { get; set; }
    }
}
