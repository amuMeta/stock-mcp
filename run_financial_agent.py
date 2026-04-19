import sys
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == "__main__":
    import uvicorn
    from financial_agent_system.main import app
    from financial_agent_system.config.settings import Settings

    settings = Settings()
    uvicorn.run(app, host=settings.host, port=settings.port, reload=settings.debug)
