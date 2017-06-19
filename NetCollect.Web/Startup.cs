using Microsoft.Owin;
using Owin;

[assembly: OwinStartupAttribute(typeof(NetCollect.Web.Startup))]
namespace NetCollect.Web
{
    public partial class Startup
    {
        public void Configuration(IAppBuilder app)
        {
            ConfigureAuth(app);
        }
    }
}
