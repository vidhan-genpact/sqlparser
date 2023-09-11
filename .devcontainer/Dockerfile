FROM mcr.microsoft.com/windows/servercore:ltsc2019

# Download and install .NET Framework 4.7.2
ADD https://download.visualstudio.microsoft.com/download/pr/ceffca5b-208d-47cd-865c-31bc0ee5e1d2/6c2b4c4b308f1c2a137a3b797d20ef9b/ndp472-web.exe C:\temp\ndp472-web.exe
RUN C:\temp\ndp472-web.exe /q /norestart

# Additional setup and installations can be added here

# Set environment variables if needed

# Set the default shell to PowerShell
SHELL ["powershell", "-Command", "$ErrorActionPreference = 'Stop'; $ProgressPreference = 'SilentlyContinue';"]

# Example: Install additional tools
# RUN Install-Package -Name <PackageName> -Source <PackageSource>

# Start a command shell
CMD ["cmd.exe"]
