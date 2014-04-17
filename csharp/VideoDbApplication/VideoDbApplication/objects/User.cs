using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace VideoDbApplication.objects
{
    public class User
    {
        public User() { }

        public User(String userName, String firstName, String lastName,
                String email, String password, DateTimeOffset createdDate,
                int totalCredits, Guid creditChangeDate)
        {
            UserName = userName;
            FirstName = firstName;
            LastName = lastName;
            Email = email;
            Password = password;
            CreatedDate = createdDate;
            TotalCredits = totalCredits;
            CreditChangeDate = creditChangeDate;
        }

        public String UserName { set; get; }
        public String FirstName { set; get; }
        public String LastName { set; get; }
        public String Email { set; get; }
        public String Password { set; get; }
        public DateTimeOffset CreatedDate { set; get; }
        public int TotalCredits { set; get; }
        public Guid CreditChangeDate { set; get; }

        public String ToString()
        {
            return "User [UserName=" + UserName + ", FirstMame=" + FirstName
                    + ", LastName=" + LastName + ", Email=" + Email + ", Password="
                    + Password + ", CreatedDate=" + CreatedDate
                    + ", TotalCredits=" + TotalCredits + ", CreditChangeDate="
                    + CreditChangeDate + "]";
        }

    }
}
